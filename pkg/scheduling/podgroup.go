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
	"fmt"

	pyv1 "github.com/kubeflow/pytorch-operator/pkg/apis/pytorch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

const (
	Legacy              = "legacy"
	Volcano             = "volcano"
	BackendAnnotation   = "arena.kubeflow.org/podgroup-backend"
	SchedulerAnnotation = "arena.kubeflow.org/podgroup-scheduler"
	GroupAnnotation     = "scheduling.k8s.io/group-name"
)

// Manager keeps PodGroup API selection independent of schedulerName.
// It never falls back to a different API after an error.
type Manager struct{ Client dynamic.Interface }

func Resource(backend string) (schema.GroupVersionResource, error) {
	switch backend {
	case Legacy:
		return schema.GroupVersionResource{Group: "scheduling.incubator.k8s.io", Version: "v1alpha1", Resource: "podgroups"}, nil
	case Volcano:
		return schema.GroupVersionResource{Group: "scheduling.volcano.sh", Version: "v1beta1", Resource: "podgroups"}, nil
	default:
		return schema.GroupVersionResource{}, fmt.Errorf("invalid PodGroup backend %q: expected legacy or volcano", backend)
	}
}

func Validate(backend, scheduler string) error {
	if _, err := Resource(backend); err != nil {
		return err
	}
	if scheduler == "" {
		return fmt.Errorf("gang scheduler name must not be empty")
	}
	return nil
}

func owned(pg *unstructured.Unstructured, job *pyv1.PyTorchJob) error {
	owner := metav1.GetControllerOf(pg)
	if job.UID == "" || owner == nil || owner.UID != job.UID || owner.Name != job.Name || owner.Kind != "PyTorchJob" || owner.APIVersion != "kubeflow.org/v1" {
		return fmt.Errorf("PodGroup %s/%s is not controlled by PyTorchJob UID %s", pg.GetNamespace(), pg.GetName(), job.UID)
	}
	return nil
}

// Resolve adopts only unambiguous owned groups. A persisted choice is authoritative.
// Both groups need get permission during adoption; authorization errors are not absence.
func (m *Manager) Resolve(job *pyv1.PyTorchJob, pods []*corev1.Pod, backend, scheduler string) (string, string, error) {
	pinned := job.Annotations[BackendAnnotation]
	if pinned != "" {
		backend, scheduler = pinned, job.Annotations[SchedulerAnnotation]
	} else {
		found := ""
		for _, candidate := range []string{Legacy, Volcano} {
			gvr, _ := Resource(candidate)
			pg, err := m.Client.Resource(gvr).Namespace(job.Namespace).Get(job.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return "", "", fmt.Errorf("inspect %s PodGroup: %v", candidate, err)
			}
			if err = owned(pg, job); err != nil {
				return "", "", err
			}
			if found != "" {
				return "", "", fmt.Errorf("both legacy and volcano PodGroups exist for %s/%s", job.Namespace, job.Name)
			}
			found = candidate
		}
		if found != "" {
			if found != backend && len(pods) == 0 {
				return "", "", fmt.Errorf("existing %s PodGroup has no Pods to infer its scheduler; explicitly select its backend and scheduler", found)
			}
			backend = found
			// Existing Pods preserve the scheduler paired with the adopted group.
			if len(pods) > 0 {
				scheduler = pods[0].Spec.SchedulerName
			}
		}
	}
	if err := Validate(backend, scheduler); err != nil {
		return "", "", err
	}
	for _, pod := range pods {
		if pod.Annotations[GroupAnnotation] != job.Name || (backend == Volcano && pod.Spec.SchedulerName != scheduler) {
			return "", "", fmt.Errorf("Pod %s has incompatible scheduler or PodGroup annotation; explicit migration required", pod.Name)
		}
	}
	if backend == Volcano {
		for role, replica := range job.Spec.PyTorchReplicaSpecs {
			if replica == nil {
				continue
			}
			name := replica.Template.Spec.SchedulerName
			if name != "" && name != scheduler {
				return "", "", fmt.Errorf("replica %s scheduler %q conflicts with gang scheduler %q", role, name, scheduler)
			}
		}
	}
	return backend, scheduler, nil
}

func (m *Manager) Ensure(job *pyv1.PyTorchJob, backend string, minMember int32) error {
	gvr, err := Resource(backend)
	if err != nil {
		return err
	}
	if job.UID == "" || minMember < 1 {
		return fmt.Errorf("PodGroup requires a job UID and positive minMember")
	}
	client := m.Client.Resource(gvr).Namespace(job.Namespace)
	pg, err := client.Get(job.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		pg = &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": gvr.GroupVersion().String(), "kind": "PodGroup",
			"metadata": map[string]interface{}{"name": job.Name, "namespace": job.Namespace},
			"spec":     map[string]interface{}{"minMember": int64(minMember)},
		}}
		pg.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(job, pyv1.SchemeGroupVersion.WithKind("PyTorchJob"))})
		_, err = client.Create(pg, metav1.CreateOptions{})
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		pg, err = client.Get(job.Name, metav1.GetOptions{})
	}
	if err != nil {
		return err
	}
	if err = owned(pg, job); err != nil {
		return err
	}
	current, _, err := unstructured.NestedInt64(pg.Object, "spec", "minMember")
	if err != nil {
		return err
	}
	if current == int64(minMember) {
		return nil
	}
	// Preserve queue, status and any other fields not managed here.
	if err = unstructured.SetNestedField(pg.Object, int64(minMember), "spec", "minMember"); err != nil {
		return err
	}
	_, err = client.Update(pg, metav1.UpdateOptions{})
	return err
}

func (m *Manager) Delete(job *pyv1.PyTorchJob, backend string) error {
	gvr, err := Resource(backend)
	if err != nil {
		return err
	}
	client := m.Client.Resource(gvr).Namespace(job.Namespace)
	pg, err := client.Get(job.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = owned(pg, job); err != nil {
		return err
	}
	uid := pg.GetUID()
	err = client.Delete(job.Name, &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
