package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func startInformers(client *kubeClient, cfg Config) error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dyn, cfg.ReconcileInterval, metav1.NamespaceAll, nil)

	backupGVR := schema.GroupVersionResource{
		Group:    backupPolicyGroup,
		Version:  backupPolicyVersion,
		Resource: "backuppolicies",
	}
	restoreGVR := schema.GroupVersionResource{
		Group:    backupPolicyGroup,
		Version:  backupPolicyVersion,
		Resource: "restorepolicies",
	}

	backupInformer := factory.ForResource(backupGVR).Informer()
	restoreInformer := factory.ForResource(restoreGVR).Informer()

	if err := attachBackupHandlers(backupInformer, client, cfg); err != nil {
		return err
	}
	if err := attachRestoreHandlers(restoreInformer, client, cfg); err != nil {
		return err
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, backupInformer.HasSynced, restoreInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for informer caches to sync")
	}
	reconcileHealthy.Store(true)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	return nil
}

func attachBackupHandlers(informer cache.SharedIndexInformer, client *kubeClient, cfg Config) error {
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			backupEventReconcile(obj, client, cfg)
		},
		UpdateFunc: func(_, newObj interface{}) {
			backupEventReconcile(newObj, client, cfg)
		},
	})
	return err
}

func attachRestoreHandlers(informer cache.SharedIndexInformer, client *kubeClient, cfg Config) error {
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			restoreEventReconcile(obj, client, cfg)
		},
		UpdateFunc: func(_, newObj interface{}) {
			restoreEventReconcile(newObj, client, cfg)
		},
	})
	return err
}

func backupEventReconcile(obj interface{}, client *kubeClient, cfg Config) {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		fmt.Println("backup event: unexpected object type")
		return
	}

	var policy BackupPolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, &policy); err != nil {
		fmt.Printf("backup event: failed to decode policy: %v\n", err)
		reconcileHealthy.Store(false)
		return
	}

	hash, err := policySpecHash(policy.Spec)
	if err != nil {
		fmt.Printf("backup event: failed to hash policy: %v\n", err)
		reconcileHealthy.Store(false)
		return
	}
	if policy.Metadata.Annotations != nil && policy.Metadata.Annotations[processedHashAnnotation] == hash {
		return
	}

	volStatus, lastSnapshotSync, err := reconcileBackupPolicy(client, cfg, policy)
	if err != nil {
		fmt.Printf("backup reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		if err := updateBackupPolicyStatus(client, policy, "False", "ReconcileError", err.Error(), volStatus, lastSnapshotSync); err != nil {
			fmt.Printf("backup status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		}
		if err := updateProcessedHash(client, "backuppolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
			fmt.Printf("backup processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		}
		reconcileHealthy.Store(false)
		return
	}
	if err := updateBackupPolicyStatus(client, policy, "True", "Reconciled", "Reconcile successful", volStatus, lastSnapshotSync); err != nil {
		fmt.Printf("backup status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		reconcileHealthy.Store(false)
		return
	}
	if err := updateProcessedHash(client, "backuppolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
		fmt.Printf("backup processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		reconcileHealthy.Store(false)
		return
	}
	reconcileHealthy.Store(true)
}

func restoreEventReconcile(obj interface{}, client *kubeClient, cfg Config) {
	unstructuredObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		fmt.Println("restore event: unexpected object type")
		return
	}

	var policy RestorePolicy
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, &policy); err != nil {
		fmt.Printf("restore event: failed to decode policy: %v\n", err)
		reconcileHealthy.Store(false)
		return
	}

	hash, err := policySpecHash(policy.Spec)
	if err != nil {
		fmt.Printf("restore event: failed to hash policy: %v\n", err)
		reconcileHealthy.Store(false)
		return
	}
	if policy.Metadata.Annotations != nil && policy.Metadata.Annotations[processedHashAnnotation] == hash {
		return
	}

	if err := reconcileRestorePolicy(client, cfg, policy); err != nil {
		fmt.Printf("restore reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		if err := updateRestoreStatus(client, policy, "False", "ReconcileError", err.Error()); err != nil {
			fmt.Printf("restore status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		}
		if err := updateProcessedHash(client, "restorepolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
			fmt.Printf("restore processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		}
		reconcileHealthy.Store(false)
		return
	}
	if err := updateRestoreStatus(client, policy, "True", "Reconciled", "Reconcile successful"); err != nil {
		fmt.Printf("restore status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		reconcileHealthy.Store(false)
		return
	}
	if err := updateProcessedHash(client, "restorepolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
		fmt.Printf("restore processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
		reconcileHealthy.Store(false)
		return
	}
	reconcileHealthy.Store(true)
}
