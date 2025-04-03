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
	//"reflect"
	//"strings"
	//"sync"

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

// +kubebuilder:rbac:groups=dcn.dcn.karmada.io,resources=pvsyncs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dcn.dcn.karmada.io,resources=pvsyncs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dcn.dcn.karmada.io,resources=pvsyncs/finalizers,verbs=update

// ms: add controller structure
type PVSyncController struct {
	client.Client
	Scheme             *runtime.Scheme
	WorkerNumber       int
	StopChan           <-chan struct{}
	PredicateFunc      predicate.Predicate
	RateLimiterOptions ratelimiterflag.Options
	worker             util.AsyncWorker //ms: modify
}

// ms: Refer to the reconcile code of endpointsliceCollectController
// ms: mainly referenced the overall part
// ms: The parts that have been changed in detail are the part that checks GVK within the work, the part that creates and registers informers, and the logic that collects PV information.
func (c *PVSyncController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).Infof("Reconciling Work %s", req.NamespacedName.String())

	work := &workv1alpha1.Work{} //ms: Preserve the part that searches the cluster for the work resource where the current event occurred
	if err := c.Client.Get(ctx, req.NamespacedName, work); err != nil {
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}
		return controllerruntime.Result{}, err
	}

	if !helper.IsWorkContains(work.Spec.Workload.Manifests, StatefulSetGVK) { //ms: fixed part to check if GVK includes the corresponding resource
		return controllerruntime.Result{}, nil
	}

	if !work.DeletionTimestamp.IsZero() {
		return controllerruntime.Result{}, nil
	}
	//ms: no need to use
	//clusterName, err := names.GetClusterName(work.Namespace) //ms: save this code because of we can get the name cluster where sts was deployed from this code
	//if err != nil {
	//	klog.Errorf("Failed to get cluster name for work %s/%s", work.Namespace, work.Name)
	//	return controllerruntime.Result{}, err
	//}

	return controllerruntime.Result{}, nil
}

// SetupWithManager creates a controller and register to controller manager.
func (c *PVSyncController) SetupWithManager(mgr controllerruntime.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		Named(PVSyncControllerName). //ms: change controller name
		For(&workv1alpha1.Work{}, builder.WithPredicates(c.PredicateFunc)).
		WithOptions(controller.Options{RateLimiter: ratelimiterflag.DefaultControllerRateLimiter[controllerruntime.Request](c.RateLimiterOptions)}).
		Complete(c)
}

// RunWorkQueue initializes worker and run it, worker will process resource asynchronously.
func (c *PVSyncController) RunWorkQueue() {
	workerOptions := util.Options{
		Name:          "persistentvolume-collect", //ms: change worker name
		KeyFunc:       nil,
		ReconcileFunc: c.collectPersistentVolume, //ms: using collectPersistentVolume function
	}
	c.worker = util.NewAsyncWorker(workerOptions)
	c.worker.Run(c.WorkerNumber, c.StopChan)
}

func (c *PVSyncController) collectPersistentVolume(key util.QueueKey) error {
	ctx := context.Background()
	fedKey, ok := key.(keys.FederatedKey)
	if !ok {
		klog.Errorf("Failed to collect persistentvolume as invalid key: %v", key)
		return fmt.Errorf("invalid key")
	}

	klog.V(4).Infof("Begin to collect persistentvolume %s.", fedKey.Name) //ms: PV is the cluster level resource so that just using fedkey.Name is okay.
	var pv corev1.PersistentVolume
	if err := c.Client.Get(ctx, types.NamespacedName{Name: fedKey.Name}, &pv); err != nil {
		klog.Errorf("Failed to get PersistentVolume %s: %v", fedKey.Name, err)
		return err
	}

	return reportPersistentVolume(ctx, c.Client, &pv, fedKey.Cluster)
}

// ms: reportPersistentVolume report PersistentVolume to control-plane.
func reportPersistentVolume(ctx context.Context, c client.Client, pv *corev1.PersistentVolume, clusterName string) error {
	executionSpace := names.GenerateExecutionSpaceName(clusterName)
	workName := names.GenerateWorkName("PersistentVolume", pv.Name, "") //ms: PV is cluster-scoped resource. so, no namespace.

	//ms: Convert to Unstructured -> 이 부분은 확실히 필요한지 다시 한번 확인
	unstructuredPV, err := helper.ToUnstructured(pv)
	if err != nil {
		klog.Errorf("Failed to convert PersistentVolume %s to unstructured, error: %v", pv.Name, err)
		return err
	}

	workMeta, err := getPersistentVolumeWorkMeta(ctx, c, executionSpace, workName, pv) //ms: generate metadata for work
	if err != nil {
		return err
	}

	// indicate the Work should be not propagated since it's collected resource.
	if err := ctrlutil.CreateOrUpdateWork(ctx, c, workMeta, unstructuredPV, ctrlutil.WithSuspendDispatching(true)); err != nil { //ms: change endpointslice to pv
		klog.Errorf("Failed to create or update work(%s/%s), Error: %v", workMeta.Namespace, workMeta.Name, err)
		return err
	}

	return nil
}

// ms: read PV info and make them into metadata that used by reportPersistentVolume to report to control-plane
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

	ls := map[string]string{
		"pv.karmada.io/claim-namespace": "", // default value
		"pv.karmada.io/claim-name":      "",
		"pv.karmada.io/storage-class":   pv.Spec.StorageClassName,
		"pv.karmada.io/managed-by":      "persistentvolume-collector",
	}

	if pv.Spec.ClaimRef != nil {
		ls["pv.karmada.io/claim-namespace"] = pv.Spec.ClaimRef.Namespace
		ls["pv.karmada.io/claim-name"] = pv.Spec.ClaimRef.Name
	}
	if pv.Spec.NFS != nil {
		ls["pv.karmada.io/nfs-server"] = pv.Spec.NFS.Server
		ls["pv.karmada.io/nfs-path"] = pv.Spec.NFS.Path
	}

	//ms: If the Work resource does not exist or the label is empty, create a new one.
	if existWork.Labels == nil || apierrors.IsNotFound(err) {
		workMeta := metav1.ObjectMeta{
			Name:      workName,
			Namespace: ns,
			Labels:    ls,
		}
		return workMeta, nil
	}

	//ms: Merge with existing labels
	ls = util.DedupeAndMergeLabels(ls, existWork.Labels)

	return metav1.ObjectMeta{
		Name:       workName,
		Namespace:  ns,
		Labels:     ls,
		Finalizers: []string{"pv.karmada.io/finalizer"}, //ms: optional
	}, nil
}
