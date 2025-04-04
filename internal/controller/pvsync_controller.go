/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	"github.com/karmada-io/karmada/pkg/controllers/ctrlutil"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/keys"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
)

var (
	StatefulSetGVK = schema.GroupVersionKind{
		Group:   "apps",
		Version: "v1",
		Kind:    "StatefulSet",
	}
)

const PVSyncControllerName = "pv-sync-controller"

// PVSyncController is the controller for syncing PV information from member clusters
type PVSyncController struct {
	client.Client
	Scheme             *runtime.Scheme
	WorkerNumber       int
	StopChan           <-chan struct{}
	PredicateFunc      predicate.Predicate
	RateLimiterOptions ratelimiterflag.Options
	worker             util.AsyncWorker
	ClusterClientFunc  func(clusterName string, controlPlaneClient client.Client) (client.Client, error)
}

// Reconcile performs a full reconciliation for the object referred to by the Request.
func (c *PVSyncController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).Infof("Reconciling Work %s", req.NamespacedName.String())

	work := &workv1alpha1.Work{}
	if err := c.Client.Get(ctx, req.NamespacedName, work); err != nil {
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}
		return controllerruntime.Result{}, err
	}

	if !helper.IsWorkContains(work.Spec.Workload.Manifests, StatefulSetGVK) {
		return controllerruntime.Result{}, nil
	}

	if !work.DeletionTimestamp.IsZero() {
		return controllerruntime.Result{}, nil
	}

	clusterName, err := names.GetClusterName(work.Namespace)
	if err != nil {
		klog.Errorf("Failed to get cluster name for work %s/%s", work.Namespace, work.Name)
		return controllerruntime.Result{}, err
	}

	c.worker.Enqueue(keys.FederatedKey{
		Cluster: clusterName,
	})

	return controllerruntime.Result{}, nil
}

// SetupWithManager creates a controller and register to controller manager.
func (c *PVSyncController) SetupWithManager(mgr controllerruntime.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		Named(PVSyncControllerName).
		For(&workv1alpha1.Work{}, builder.WithPredicates(c.PredicateFunc)).
		WithOptions(controller.Options{RateLimiter: ratelimiterflag.DefaultControllerRateLimiter[controllerruntime.Request](c.RateLimiterOptions)}).
		Complete(c)
}

// RunWorkQueue initializes worker and run it, worker will process resource asynchronously.
func (c *PVSyncController) RunWorkQueue() {
	workerOptions := util.Options{
		Name:          "persistentvolume-collect",
		KeyFunc:       nil,
		ReconcileFunc: c.collectPersistentVolume,
	}
	c.worker = util.NewAsyncWorker(workerOptions)
	c.worker.Run(c.WorkerNumber, c.StopChan)
}

// collectPersistentVolume collects PersistentVolume information from member clusters
func (c *PVSyncController) collectPersistentVolume(key util.QueueKey) error {
	ctx := context.Background()

	fedKey, ok := key.(keys.FederatedKey)
	if !ok {
		return fmt.Errorf("invalid key type: %v", key)
	}
	clusterName := fedKey.Cluster
	klog.V(4).Infof("Collecting PersistentVolumes from cluster %s", clusterName)

	// Get client for the member cluster
	clusterClient, err := c.ClusterClientFunc(clusterName, c.Client)
	if err != nil {
		klog.Errorf("Failed to get client for cluster %s: %v", clusterName, err)
		return err
	}

	// List StatefulSets in the member cluster
	stsList := &appsv1.StatefulSetList{}
	if err := clusterClient.List(ctx, stsList); err != nil {
		klog.Errorf("Failed to list StatefulSets in cluster %s: %v", clusterName, err)
		return err
	}

	// Process each StatefulSet
	for _, sts := range stsList.Items {
		if len(sts.Spec.VolumeClaimTemplates) == 0 {
			continue
		}

		// Get PVCs for this StatefulSet
		for _, template := range sts.Spec.VolumeClaimTemplates {
			// StatefulSet PVCs follow the naming convention: <volume-name>-<statefulset-name>-<ordinal>
			// List all PVCs in the StatefulSet's namespace that match this pattern
			pvcList := &corev1.PersistentVolumeClaimList{}
			if err := clusterClient.List(ctx, pvcList, client.InNamespace(sts.Namespace)); err != nil {
				klog.Errorf("Failed to list PVCs in namespace %s: %v", sts.Namespace, err)
				continue
			}

			// Filter PVCs that belong to this StatefulSet's VolumeClaimTemplate
			for _, pvc := range pvcList.Items {
				// Skip PVCs that don't have a volume name assigned
				if pvc.Spec.VolumeName == "" {
					continue
				}

				// Check if this PVC belongs to our StatefulSet
				// A simple check would be to see if the PVC name starts with the template name and the StatefulSet name
				if !isPVCBelongsToStatefulSet(pvc.Name, template.Name, sts.Name) {
					continue
				}

				// Get the PV for this PVC
				pv := &corev1.PersistentVolume{}
				if err := clusterClient.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
					klog.Errorf("Failed to get PV %s: %v", pvc.Spec.VolumeName, err)
					continue
				}

				// Skip if not NFS type (for now)
				if pv.Spec.NFS == nil {
					klog.V(4).Infof("Skipping non-NFS PV %s", pv.Name)
					continue
				}

				// Report this PV to the control plane
				if err := reportPersistentVolume(ctx, c.Client, pv, clusterName); err != nil {
					klog.Warningf("Failed to report PV %s: %v", pv.Name, err)
				} else {
					klog.V(4).Infof("Successfully reported PV %s from cluster %s", pv.Name, clusterName)
				}
			}
		}
	}

	return nil
}

// isPVCBelongsToStatefulSet checks if a PVC belongs to a StatefulSet based on naming convention
// StatefulSet PVCs follow the naming convention: <volume-name>-<statefulset-name>-<ordinal>
func isPVCBelongsToStatefulSet(pvcName, volumeName, stsName string) bool {
	// Simple check: PVC name should start with volumeName-stsName-
	prefix := volumeName + "-" + stsName + "-"
	return len(pvcName) > len(prefix) && pvcName[:len(prefix)] == prefix
}

// reportPersistentVolume reports PersistentVolume to control-plane
func reportPersistentVolume(ctx context.Context, c client.Client, pv *corev1.PersistentVolume, clusterName string) error {
	executionSpace := names.GenerateExecutionSpaceName(clusterName)
	workName := names.GenerateWorkName("PersistentVolume", pv.Name, "")

	// Convert to Unstructured
	unstructuredPV, err := helper.ToUnstructured(pv)
	if err != nil {
		klog.Errorf("Failed to convert PersistentVolume %s to unstructured, error: %v", pv.Name, err)
		return err
	}

	// Get metadata for work
	workMeta, err := getPersistentVolumeWorkMeta(ctx, c, executionSpace, workName, pv)
	if err != nil {
		return err
	}

	// Create or update work
	if err := ctrlutil.CreateOrUpdateWork(ctx, c, workMeta, unstructuredPV, ctrlutil.WithSuspendDispatching(true)); err != nil {
		klog.Errorf("Failed to create or update work(%s/%s), Error: %v", workMeta.Namespace, workMeta.Name, err)
		return err
	}

	return nil
}

// getPersistentVolumeWorkMeta creates metadata for the work resource
func getPersistentVolumeWorkMeta(ctx context.Context, c client.Client, ns string, workName string, pv *corev1.PersistentVolume) (metav1.ObjectMeta, error) {
	existWork := &workv1alpha1.Work{}
	var err error
	if err = c.Get(ctx, types.NamespacedName{
		Namespace: ns,
		Name:      workName,
	}, existWork); err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Get PersistentVolume work(%s/%s) error:%v", ns, workName, err)
		return metav1.ObjectMeta{}, err
	}

	// Create labels for the work resource
	ls := map[string]string{
		"pv.karmada.io/claim-namespace": "", // default value
		"pv.karmada.io/claim-name":      "",
		"pv.karmada.io/managed-by":      "persistentvolume-collector",
	}

	// Set storage class if available
	if pv.Spec.StorageClassName != "" {
		ls["pv.karmada.io/storage-class"] = pv.Spec.StorageClassName
	}

	// Set claim information if available
	if pv.Spec.ClaimRef != nil {
		ls["pv.karmada.io/claim-namespace"] = pv.Spec.ClaimRef.Namespace
		ls["pv.karmada.io/claim-name"] = pv.Spec.ClaimRef.Name
	}

	// Set NFS information if available
	if pv.Spec.NFS != nil {
		ls["pv.karmada.io/nfs-server"] = pv.Spec.NFS.Server
		ls["pv.karmada.io/nfs-path"] = pv.Spec.NFS.Path
	}

	// If the Work resource does not exist or the label is empty, create a new one
	if existWork.Labels == nil || apierrors.IsNotFound(err) {
		workMeta := metav1.ObjectMeta{
			Name:      workName,
			Namespace: ns,
			Labels:    ls,
		}
		return workMeta, nil
	}

	// Merge with existing labels
	ls = util.DedupeAndMergeLabels(ls, existWork.Labels)

	return metav1.ObjectMeta{
		Name:      workName,
		Namespace: ns,
		Labels:    ls,
	}, nil
}
