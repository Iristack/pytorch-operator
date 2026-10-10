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

package pytorch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kubeflow/pytorch-operator/cmd/pytorch-operator.v1/app/options"
	pyv1 "github.com/kubeflow/pytorch-operator/pkg/apis/pytorch/v1"
	jobfake "github.com/kubeflow/pytorch-operator/pkg/client/clientset/versioned/fake"
	jobscheme "github.com/kubeflow/pytorch-operator/pkg/client/clientset/versioned/scheme"
	"github.com/kubeflow/pytorch-operator/pkg/common/util/v1/testutil"
	"github.com/kubeflow/pytorch-operator/pkg/scheduling"
	kubebatch "github.com/kubernetes-sigs/kube-batch/pkg/client/clientset/versioned"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"
)

func gangTestController(t *testing.T, job *pyv1.PyTorchJob, host string, enabled bool) (*PyTorchController, *jobfake.Clientset) {
	t.Helper()
	jobscheme.Scheme.Default(job)
	jobClient := jobfake.NewSimpleClientset(job.DeepCopy())
	config := &rest.Config{Host: host}
	pc, _, _ := newPyTorchController(config, kubeclient.NewForConfigOrDie(config), kubebatch.NewForConfigOrDie(config), jobClient, controller.NoResyncPeriodFunc, options.ServerOption{EnableGangScheduling: enabled, GangSchedulerName: "volcano", PodGroupBackend: scheduling.Volcano})
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	pc.SetPodGroupClient(client)
	pc.Recorder = record.NewFakeRecorder(20)
	pc.updateStatusHandler = func(*pyv1.PyTorchJob) error { return nil }
	return pc, jobClient
}

func TestGangReconcileRequiresPodGroup(t *testing.T) {
	for _, tc := range []struct {
		name                                   string
		denyGet, denyCreate, denyPin, disabled bool
		workers                                int32
	}{
		{name: "single node"}, {name: "distributed", workers: 2},
		{name: "get forbidden", denyGet: true}, {name: "create forbidden", denyCreate: true},
		{name: "pin conflict", denyPin: true}, {name: "gang disabled", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := testutil.NewPyTorchJobWithMaster(int(tc.workers))
			job.UID = "job-1"
			job.ResourceVersion = "10"
			groupCreated := false
			apiRequests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiRequests++
				w.Header().Set("Content-Type", "application/json")
				if tc.denyGet || (tc.denyCreate && r.Method == "POST") {
					w.WriteHeader(403)
					fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","reason":"Forbidden","code":403}`)
					return
				}
				if r.Method == "GET" {
					w.WriteHeader(404)
					fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","reason":"NotFound","code":404}`)
					return
				}
				if r.Method != "POST" || !strings.Contains(r.URL.Path, "scheduling.volcano.sh/v1beta1") {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				var pg map[string]interface{}
				json.NewDecoder(r.Body).Decode(&pg)
				if pg["spec"].(map[string]interface{})["minMember"] != float64(tc.workers+1) {
					t.Errorf("wrong minMember %v", pg)
				}
				groupCreated = true
				w.WriteHeader(201)
				json.NewEncoder(w).Encode(pg)
			}))
			defer server.Close()
			pc, jobClient := gangTestController(t, job, server.URL, !tc.disabled)
			if tc.denyPin {
				jobClient.PrependReactor("patch", "pytorchjobs", func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, fmt.Errorf("resourceVersion conflict")
				})
			}
			err := pc.reconcilePyTorchJobs(job)
			failed := tc.denyGet || tc.denyCreate || tc.denyPin
			if (err != nil) != failed {
				t.Fatalf("err=%v expected failure=%v", err, failed)
			}
			pods := pc.PodControl.(*controller.FakePodControl)
			if failed {
				if pods.CreateCallCount != 0 {
					t.Fatal("created pods without PodGroup")
				}
				return
			}
			if pods.CreateCallCount != int(tc.workers+1) {
				t.Fatalf("created %d pods", pods.CreateCallCount)
			}
			if tc.disabled {
				if apiRequests != 0 {
					t.Fatal("gang disabled accessed PodGroup API")
				}
				return
			}
			if !groupCreated {
				t.Fatal("missing group")
			}
			for _, pod := range pods.Templates {
				if pod.Spec.SchedulerName != "volcano" || pod.Annotations[scheduling.GroupAnnotation] != job.Name {
					t.Fatalf("wrong Pod scheduling: %+v", pod)
				}
			}
			saved, err := jobClient.KubeflowV1().PyTorchJobs(job.Namespace).Get(job.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if saved.Annotations[scheduling.BackendAnnotation] != scheduling.Volcano {
				t.Fatal("backend not persisted")
			}
		})
	}
}

func TestGangPinnedBackendSurvivesRestartAndDisable(t *testing.T) {
	job := testutil.NewPyTorchJobWithMaster(0)
	job.UID = "job-1"
	job.Annotations = map[string]string{scheduling.BackendAnnotation: scheduling.Volcano, scheduling.SchedulerAnnotation: "volcano"}
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","reason":"NotFound","code":404}`)
			return
		}
		var pg map[string]interface{}
		json.NewDecoder(r.Body).Decode(&pg)
		json.NewEncoder(w).Encode(pg)
	}))
	defer server.Close()
	pc, _ := gangTestController(t, job, server.URL, false)
	pc.podGroupBackend = scheduling.Legacy
	pc.Config.GangSchedulerName = "kube-batch"
	if err := pc.reconcilePyTorchJobs(job); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("pinned gang ignored")
	}
	for _, path := range paths {
		if !strings.Contains(path, "scheduling.volcano.sh/v1beta1") {
			t.Fatal(path)
		}
	}
	if pc.PodControl.(*controller.FakePodControl).Templates[0].Spec.SchedulerName != "volcano" {
		t.Fatal("scheduler changed")
	}
}

func TestGangBindingRejectsTemplateConflict(t *testing.T) {
	job := testutil.NewPyTorchJobWithMaster(0)
	job.UID = "job-1"
	job.Annotations = map[string]string{scheduling.BackendAnnotation: scheduling.Volcano, scheduling.SchedulerAnnotation: "volcano"}
	job.Spec.PyTorchReplicaSpecs[pyv1.PyTorchReplicaTypeMaster].Template.Spec.SchedulerName = "default-scheduler"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected API access") }))
	defer server.Close()
	pc, _ := gangTestController(t, job, server.URL, true)
	if err := pc.reconcilePyTorchJobs(job); err == nil {
		t.Fatal("expected scheduler conflict")
	}
	if pc.PodControl.(*controller.FakePodControl).CreateCallCount != 0 {
		t.Fatal("created conflicting pod")
	}

}

func TestGangCleanupUsesPinnedBackend(t *testing.T) {
	job := testutil.NewPyTorchJobWithMaster(0)
	job.UID = "job-1"
	job.Annotations = map[string]string{scheduling.BackendAnnotation: scheduling.Volcano, scheduling.SchedulerAnnotation: "volcano"}
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.Contains(r.URL.Path, "scheduling.volcano.sh/v1beta1") {
			t.Errorf("wrong cleanup backend %s", r.URL.Path)
		}
		switch r.Method {
		case "GET":
			fmt.Fprintf(w, `{"apiVersion":"scheduling.volcano.sh/v1beta1","kind":"PodGroup","metadata":{"name":%q,"namespace":%q,"uid":"group-1","ownerReferences":[{"apiVersion":"kubeflow.org/v1","kind":"PyTorchJob","name":%q,"uid":"job-1","controller":true}]}}`, job.Name, job.Namespace, job.Name)
		case "DELETE":
			deleted = true
			fmt.Fprint(w, `{"apiVersion":"v1","kind":"Status","status":"Success"}`)
		default:
			t.Errorf("unexpected cleanup method %s", r.Method)
		}
	}))
	defer server.Close()
	pc, _ := gangTestController(t, job, server.URL, false)
	pc.podGroupBackend = scheduling.Legacy
	if err := pc.deleteGangPodGroup(job); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("group not deleted")
	}
}
