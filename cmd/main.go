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

package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlruntimelog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/gclient"
	"github.com/karmada-io/karmada/pkg/util/names"
	"github.com/karmada-io/karmada/pkg/util/objectwatcher"

	"github.com/karmada-io/karmada/pkg/controllers"
	"github.com/statlove/internal/controller"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrlruntimelog.Log.WithName("setup")

	restConfigQPS   = flag.Int("rest-config-qps", 30, "QPS of rest config.")
	restConfigBurst = flag.Int("rest-config-burst", 50, "Burst of rest config.")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(clusterv1alpha1.AddToScheme(scheme))
	utilruntime.Must(workv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var workerNumber int
	var rateLimiterOpts ratelimiterflag.Options

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.IntVar(&workerNumber, "worker-number", 10, "Number of worker threads for controller.")
	ratelimiterflag.AddFlags(flag.CommandLine, &rateLimiterOpts)

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrlruntimelog.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Setup controller manager
	mgr, err := controllerruntime.NewManager(controllerruntime.GetConfigOrDie(), controllerruntime.Options{
		Scheme:                 scheme,
		MetricsBindAddress:     metricsAddr,
		Port:                   9443,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "pv-sync-controller.karmada.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	controlPlaneRestConfig := mgr.GetConfig()
	controlPlaneRestConfig.QPS = float32(*restConfigQPS)
	controlPlaneRestConfig.Burst = *restConfigBurst

	// Initialize the controller
	pvSyncController := &controller.PVSyncController{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		WorkerNumber:       workerNumber,
		StopChan:           controllerruntime.SetupSignalHandler(),
		PredicateFunc:      controllers.BuildPredicateByResource(controllers.WorkKind),
		RateLimiterOptions: rateLimiterOpts,
		ClusterClientFunc:  getClusterClient,
	}

	if err = pvSyncController.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to setup controller", "controller", "PVSyncController")
		os.Exit(1)
	}

	// Start the worker queue
	go pvSyncController.RunWorkQueue()

	// Setup health checks
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(controllerruntime.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// getClusterClient returns a client for the member cluster
func getClusterClient(clusterName string, controlPlaneClient client.Client) (client.Client, error) {
	cluster := &clusterv1alpha1.Cluster{}
	if err := controlPlaneClient.Get(controllerruntime.GetContextWithValue(), 
		client.ObjectKey{Name: clusterName}, cluster); err != nil {
		return nil, err
	}

	clusterConfig, err := util.BuildClusterConfig(cluster, controlPlaneClient, nil)
	if err != nil {
		return nil, err
	}

	return gclient.NewForConfig(clusterConfig)
}
