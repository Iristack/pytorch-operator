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

	pyv1 "github.com/kubeflow/pytorch-operator/pkg/apis/pytorch/v1"
	"github.com/kubeflow/pytorch-operator/pkg/scheduling"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

func (pc *PyTorchController) SetPodGroupClient(client dynamic.Interface) {
	pc.podGroupManager = &scheduling.Manager{Client: client}
}

func (pc *PyTorchController) gangEnabled(job *pyv1.PyTorchJob) bool {
	return pc.Config.EnableGangScheduling || job.Annotations[scheduling.BackendAnnotation] != ""
}

func (pc *PyTorchController) gangScheduler(job *pyv1.PyTorchJob) string {
	if job.Annotations[scheduling.BackendAnnotation] != "" {
		return job.Annotations[scheduling.SchedulerAnnotation]
	}
	return pc.Config.GangSchedulerName
}

// Persist the selection before creating any child resource. ResourceVersion makes
// competing writes fail rather than changing the backend of an existing task.
func (pc *PyTorchController) bindPodGroupBackend(job *pyv1.PyTorchJob, pods []*corev1.Pod) error {
	if pc.podGroupManager == nil {
		return fmt.Errorf("PodGroup client is not configured")
	}
	backend := pc.podGroupBackend
	if backend == "" {
		backend = scheduling.Legacy
	}
	backend, scheduler, err := pc.podGroupManager.Resolve(job, pods, backend, pc.Config.GangSchedulerName)
	if err != nil {
		return err
	}
	if job.Annotations[scheduling.BackendAnnotation] != "" {
		return nil
	}
	annotations := map[string]string{scheduling.BackendAnnotation: backend, scheduling.SchedulerAnnotation: scheduler}
	patch, err := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{
		"resourceVersion": job.ResourceVersion, "annotations": annotations,
	}})
	if err != nil {
		return err
	}
	updated, err := pc.jobClientSet.KubeflowV1().PyTorchJobs(job.Namespace).Patch(job.Name, types.MergePatchType, patch)
	if err != nil {
		return fmt.Errorf("persist PodGroup backend: %v", err)
	}
	job.ResourceVersion = updated.ResourceVersion
	if job.Annotations == nil {
		job.Annotations = make(map[string]string)
	}
	for key, value := range annotations {
		job.Annotations[key] = value
	}
	pc.Recorder.Eventf(job, corev1.EventTypeNormal, "PodGroupBackendSelected", "backend=%s scheduler=%s", backend, scheduler)
	return nil
}

func (pc *PyTorchController) deleteGangPodGroup(job *pyv1.PyTorchJob) error {
	if pc.podGroupManager == nil {
		return fmt.Errorf("PodGroup client is not configured")
	}
	backend := job.Annotations[scheduling.BackendAnnotation]
	err := pc.podGroupManager.Delete(job, backend)
	if err != nil {
		pc.Recorder.Eventf(job, corev1.EventTypeWarning, "PodGroupDeleteFailed", "backend=%s: %v", backend, err)
	}
	return err
}
