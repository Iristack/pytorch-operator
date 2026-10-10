// Copyright 2026 The Arena Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduling

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	common "github.com/kubeflow/common/job_controller/api/v1"
	pyv1 "github.com/kubeflow/pytorch-operator/pkg/apis/pytorch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func testJob() *pyv1.PyTorchJob {
	return &pyv1.PyTorchJob{ObjectMeta: metav1.ObjectMeta{Name: "train", Namespace: "team", UID: types.UID("job-1")}}
}
func testGroup(job *pyv1.PyTorchJob, backend string) *unstructured.Unstructured {
	gvr, _ := Resource(backend)
	p := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": gvr.GroupVersion().String(), "kind": "PodGroup", "spec": map[string]interface{}{"minMember": int64(1), "queue": "research"},
	}}
	p.SetName(job.Name)
	p.SetNamespace(job.Namespace)
	p.SetUID("group-1")
	p.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(job, pyv1.SchemeGroupVersion.WithKind("PyTorchJob"))})
	return p
}
func testManager(t *testing.T, handler http.HandlerFunc) (*Manager, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	client, err := dynamic.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return &Manager{Client: client}, server.Close
}
func status(w http.ResponseWriter, code int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReason(reason), Code: int32(code)})
}

func TestPodGroupAPILifecycle(t *testing.T) {
	for _, backend := range []string{Legacy, Volcano} {
		t.Run(backend, func(t *testing.T) {
			job := testJob()
			gvr, _ := Resource(backend)
			path := "/apis/" + gvr.GroupVersion().String() + "/namespaces/team/podgroups"
			var stored *unstructured.Unstructured
			creates, updates, deletes := 0, 0, 0
			manager, closeServer := testManager(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != path && r.URL.Path != path+"/train" {
					t.Errorf("wrong API path %s", r.URL.Path)
					status(w, 404, "NotFound")
					return
				}
				switch r.Method {
				case "GET":
					if stored == nil {
						status(w, 404, "NotFound")
						return
					}
					json.NewEncoder(w).Encode(stored)
				case "POST":
					creates++
					stored = &unstructured.Unstructured{}
					json.NewDecoder(r.Body).Decode(stored)
					stored.SetUID("group-1")
					w.WriteHeader(201)
					json.NewEncoder(w).Encode(stored)
				case "PUT":
					updates++
					stored = &unstructured.Unstructured{}
					json.NewDecoder(r.Body).Decode(stored)
					json.NewEncoder(w).Encode(stored)
				case "DELETE":
					var opts metav1.DeleteOptions
					json.NewDecoder(r.Body).Decode(&opts)
					if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "group-1" {
						t.Error("missing UID delete precondition")
					}
					deletes++
					stored = nil
					json.NewEncoder(w).Encode(&metav1.Status{Status: "Success"})
				default:
					t.Errorf("unexpected method %s", r.Method)
				}
			})
			defer closeServer()
			if err := manager.Ensure(job, backend, 3); err != nil {
				t.Fatal(err)
			}
			if err := owned(stored, job); err != nil {
				t.Fatal(err)
			}
			if stored.GetAPIVersion() != gvr.GroupVersion().String() {
				t.Fatal(stored)
			}
			unstructured.SetNestedField(stored.Object, "research", "spec", "queue")
			if err := manager.Ensure(job, backend, 3); err != nil {
				t.Fatal(err)
			}
			if err := manager.Ensure(job, backend, 4); err != nil {
				t.Fatal(err)
			}
			queue, _, _ := unstructured.NestedString(stored.Object, "spec", "queue")
			if creates != 1 || updates != 1 || queue != "research" {
				t.Fatalf("creates=%d updates=%d queue=%s", creates, updates, queue)
			}
			if err := manager.Delete(job, backend); err != nil {
				t.Fatal(err)
			}
			if err := manager.Delete(job, backend); err != nil {
				t.Fatal(err)
			}
			if deletes != 1 {
				t.Fatal(deletes)
			}
		})
	}
}

func TestPodGroupErrorsDoNotCreateOrFallback(t *testing.T) {
	for _, code := range []int{403, 500} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			requests := 0
			manager, stop := testManager(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != "GET" || !strings.Contains(r.URL.Path, "scheduling.volcano.sh") {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				reason := "InternalError"
				if code == 403 {
					reason = "Forbidden"
				}
				status(w, code, reason)
			})
			defer stop()
			if err := manager.Ensure(testJob(), Volcano, 1); err == nil {
				t.Fatal("expected error")
			}
			if requests != 1 {
				t.Fatalf("unexpected retry/fallback: %d requests", requests)
			}
		})
	}
}

func TestPodGroupRejectsForeignOwner(t *testing.T) {
	job := testJob()
	foreign := testGroup(job, Volcano)
	foreign.SetOwnerReferences(nil)
	mutations := 0
	manager, stop := testManager(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			mutations++
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(foreign)
	})
	defer stop()
	if err := manager.Ensure(job, Volcano, 2); err == nil {
		t.Fatal("adopted foreign group")
	}
	if err := manager.Delete(job, Volcano); err == nil {
		t.Fatal("deleted foreign group")
	}
	if mutations != 0 {
		t.Fatal(mutations)
	}
}

func TestPodGroupResolve(t *testing.T) {
	for _, tc := range []struct {
		name, found, pinned, expect              string
		conflict, forbidden, badPod, badTemplate bool
	}{
		{name: "new volcano", expect: Volcano},
		{name: "adopt legacy", found: Legacy, expect: Legacy},
		{name: "adopt volcano", found: Volcano, expect: Volcano},
		{name: "pinned survives config", pinned: Legacy, expect: Legacy},
		{name: "both APIs", found: Legacy, conflict: true},
		{name: "ambiguous scheduler without pods", found: Legacy},
		{name: "forbidden is not absent", forbidden: true},
		{name: "pending pod repair", expect: Volcano},
		{name: "incompatible pending pod", badPod: true},
		{name: "template scheduler conflict", badTemplate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := testJob()
			requests := 0
			if tc.pinned != "" {
				job.Annotations = map[string]string{BackendAnnotation: tc.pinned, SchedulerAnnotation: "volcano"}
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "train-master-0", Annotations: map[string]string{GroupAnnotation: "train"}}, Spec: corev1.PodSpec{SchedulerName: "volcano"}}
			if tc.badPod {
				pod.Spec.SchedulerName = "default-scheduler"
			}
			var pods []*corev1.Pod
			if tc.name == "pending pod repair" || tc.name == "adopt legacy" || tc.badPod {
				pods = []*corev1.Pod{pod}
			}
			if tc.badTemplate {
				job.Spec.PyTorchReplicaSpecs = map[pyv1.PyTorchReplicaType]*common.ReplicaSpec{pyv1.PyTorchReplicaTypeMaster: {Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{SchedulerName: "other"}}}}
			}
			manager, stop := testManager(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				if tc.forbidden {
					status(w, 403, "Forbidden")
					return
				}
				b := Legacy
				if strings.Contains(r.URL.Path, "scheduling.volcano.sh") {
					b = Volcano
				}
				if tc.found != b && !tc.conflict {
					status(w, 404, "NotFound")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(testGroup(job, b))
			})
			defer stop()
			backend, _, err := manager.Resolve(job, pods, Volcano, "volcano")
			if tc.expect == "" {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil || backend != tc.expect {
				t.Fatalf("backend=%s err=%v", backend, err)
			}
			if tc.pinned != "" && requests != 0 {
				t.Fatal("pinned job re-probed API")
			}
		})
	}
}

func TestPodGroupCreateRaceChecksOwner(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "foreign"}[foreign], func(t *testing.T) {
			job := testJob()
			gets := 0
			manager, stop := testManager(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					status(w, 409, "AlreadyExists")
					return
				}
				gets++
				if gets == 1 {
					status(w, 404, "NotFound")
					return
				}
				pg := testGroup(job, Volcano)
				if foreign {
					pg.SetOwnerReferences(nil)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(pg)
			})
			defer stop()
			err := manager.Ensure(job, Volcano, 1)
			if (err != nil) != foreign {
				t.Fatalf("foreign=%v error=%v", foreign, err)
			}
		})
	}
}
