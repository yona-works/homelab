package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type RestorePolicyHandler struct{}

func (h *RestorePolicyHandler) Reconcile(client *kubeClient, cfg Config) error {
	listPath := fmt.Sprintf("/apis/%s/%s/restorepolicies", backupPolicyGroup, backupPolicyVersion)
	body, status, err := client.doRequest("GET", listPath, nil)
	if err != nil {
		fmt.Printf("failed to list RestorePolicies: %v\n", err)
		return err
	}
	if status != http.StatusOK {
		err := fmt.Errorf("failed to list RestorePolicies: status=%d", status)
		fmt.Println(err.Error())
		return err
	}

	var list RestorePolicyList
	if err := json.Unmarshal(body, &list); err != nil {
		fmt.Printf("failed to parse RestorePolicies: %v\n", err)
		return err
	}
	fmt.Printf("reconcile: found %d RestorePolicies\n", len(list.Items))

	for _, policy := range list.Items {
		hash, err := policySpecHash(policy.Spec)
		if err != nil {
			fmt.Printf("restore reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			return err
		}
		if policy.Metadata.Annotations != nil && policy.Metadata.Annotations[processedHashAnnotation] == hash {
			continue
		}

		if err := reconcileRestorePolicy(client, cfg, policy); err != nil {
			fmt.Printf("restore reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			if err := updateRestoreStatus(client, policy, "False", "ReconcileError", err.Error()); err != nil {
				fmt.Printf("restore status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				return err
			}
			if err := updateProcessedHash(client, "restorepolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
				fmt.Printf("processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				return err
			}
			return err
		}
		if err := updateRestoreStatus(client, policy, "True", "Reconciled", "Reconcile successful"); err != nil {
			fmt.Printf("restore status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			return err
		}
		if err := updateProcessedHash(client, "restorepolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
			fmt.Printf("processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			return err
		}
	}

	return nil
}

func reconcileRestorePolicy(client *kubeClient, cfg Config, policy RestorePolicy) error {
	ns := policy.Metadata.Namespace
	name := policy.Metadata.Name
	if policy.APIVersion == "" {
		policy.APIVersion = fmt.Sprintf("%s/%s", backupPolicyGroup, backupPolicyVersion)
	}
	if policy.Kind == "" {
		policy.Kind = "RestorePolicy"
	}

	if policy.Spec.SourceNamespace == "" {
		return fmt.Errorf("spec.sourceNamespace is required")
	}

	for _, vol := range policy.Spec.Volumes {
		if vol.SourcePVC == "" || vol.TargetPVC == "" {
			continue
		}
		secretName := sanitizeName(fmt.Sprintf("restore-repo-%s-%s", name, vol.SourcePVC))
		if err := ensureRestoreExternalSecret(client, cfg, ns, secretName, policy.Spec.SourceNamespace, vol.SourcePVC, policy); err != nil {
			return err
		}

		if err := ensureTargetPVC(client, cfg, ns, policy.Spec.SourceNamespace, vol.SourcePVC, vol.TargetPVC); err != nil {
			return err
		}

		restoreName := sanitizeName(fmt.Sprintf("restore-%s-%s", name, vol.TargetPVC))
		if err := ensureReplicationDestination(client, cfg, ns, restoreName, secretName, vol.TargetPVC, vol.RestoreAsOf, policy); err != nil {
			return err
		}
	}

	return nil
}

func ensureReplicationDestination(client *kubeClient, cfg Config, ns, name, secretName, pvc, restoreAsOf string, policy RestorePolicy) error {
	resticSpec := map[string]interface{}{
		"repository":     secretName,
		"copyMethod":     "Direct",
		"destinationPVC": pvc,
	}
	if restoreAsOf != "" {
		resticSpec["restoreAsOf"] = restoreAsOf
	}
	mountPath := cfg.RepoMountPath
	resticSpec["moverVolumes"] = []map[string]interface{}{
		{
			"mountPath": mountPath,
			"volumeSource": map[string]interface{}{
				"persistentVolumeClaim": map[string]interface{}{
					"claimName": cfg.RepoPVCName,
					"readOnly":  false,
				},
			},
		},
	}

	obj := map[string]interface{}{
		"apiVersion": "volsync.backube/v1alpha1",
		"kind":       "ReplicationDestination",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ns,
			"labels": map[string]interface{}{
				"restore-policy/name":      policy.Metadata.Name,
				"restore-policy/namespace": ns,
			},
		},
		"spec": map[string]interface{}{
			"trigger": map[string]interface{}{
				"manual": "init",
			},
			"restic": resticSpec,
		},
	}

	return client.upsert(namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationdestinations", name),
		namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationdestinations"), obj, nil)
}

func ensureRestoreExternalSecret(client *kubeClient, cfg Config, ns, secretName, sourceNamespace, sourcePVC string, policy RestorePolicy) error {
	secretData := []map[string]interface{}{
		{
			"remoteRef": map[string]interface{}{"key": cfg.ExternalSecretKey, "property": cfg.ResticPasswordProperty},
			"secretKey": "restic_password",
		},
	}

	templateData := map[string]interface{}{
		"RESTIC_PASSWORD":   "{{ .restic_password }}",
		"RESTIC_REPOSITORY": fmt.Sprintf("/mnt/%s/%s/%s", cfg.RepoMountPath, sourceNamespace, sourcePVC),
	}

	obj := map[string]interface{}{
		"apiVersion": "external-secrets.io/v1beta1",
		"kind":       "ExternalSecret",
		"metadata": map[string]interface{}{
			"name":      secretName,
			"namespace": ns,
			"labels": map[string]interface{}{
				"restore-policy/name":      policy.Metadata.Name,
				"restore-policy/namespace": ns,
			},
		},
		"spec": map[string]interface{}{
			"secretStoreRef": map[string]interface{}{
				"kind": cfg.ExternalSecretStoreKind,
				"name": cfg.ExternalSecretStoreName,
			},
			"data": secretData,
			"target": map[string]interface{}{
				"template": map[string]interface{}{
					"data": templateData,
				},
			},
		},
	}

	return client.upsert(namespacedPath("/apis/external-secrets.io/v1beta1", ns, "externalsecrets", secretName),
		namespacedPath("/apis/external-secrets.io/v1beta1", ns, "externalsecrets"), obj, nil)
}

func ensureTargetPVC(client *kubeClient, cfg Config, targetNamespace, sourceNamespace, sourcePVC, targetPVC string) error {
	itemPath := namespacedPath("/api/v1", targetNamespace, "persistentvolumeclaims", targetPVC)
	_, status, err := client.doRequest("GET", itemPath, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		return nil
	}
	if status != http.StatusNotFound {
		return fmt.Errorf("get failed: %s status=%d", itemPath, status)
	}

	srcPath := namespacedPath("/api/v1", sourceNamespace, "persistentvolumeclaims", sourcePVC)
	srcBody, srcStatus, err := client.doRequest("GET", srcPath, nil)
	if err != nil {
		return err
	}
	if srcStatus != http.StatusOK {
		return fmt.Errorf("get failed: %s status=%d", srcPath, srcStatus)
	}

	var src map[string]interface{}
	if err := json.Unmarshal(srcBody, &src); err != nil {
		return err
	}
	srcSpec, _ := src["spec"].(map[string]interface{})
	srcResources, _ := srcSpec["resources"].(map[string]interface{})
	srcRequests, _ := srcResources["requests"].(map[string]interface{})
	srcStorage := srcRequests["storage"]

	srcAccessModes, _ := srcSpec["accessModes"].([]interface{})
	srcStorageClass, _ := srcSpec["storageClassName"].(string)

	pvc := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]interface{}{
			"name":      targetPVC,
			"namespace": targetNamespace,
		},
		"spec": map[string]interface{}{
			"accessModes": srcAccessModes,
			"resources": map[string]interface{}{
				"requests": map[string]interface{}{
					"storage": srcStorage,
				},
			},
		},
	}
	if srcStorageClass != "" {
		pvc["spec"].(map[string]interface{})["storageClassName"] = srcStorageClass
	}

	_, createStatus, err := client.doRequest("POST", namespacedPath("/api/v1", targetNamespace, "persistentvolumeclaims"), pvc)
	if err != nil {
		return err
	}
	if createStatus < 200 || createStatus >= 300 {
		return fmt.Errorf("create failed: %s status=%d", targetNamespace, createStatus)
	}
	return nil
}

func updateRestoreStatus(client *kubeClient, policy RestorePolicy, status, reason, message string) error {
	if policy.Metadata.Name == "" || policy.Metadata.Namespace == "" {
		return fmt.Errorf("missing policy name/namespace for status update")
	}
	statusPath := namespacedPath(
		fmt.Sprintf("/apis/%s/%s", backupPolicyGroup, backupPolicyVersion),
		policy.Metadata.Namespace,
		"restorepolicies",
		policy.Metadata.Name,
	) + "/status"

	condition := map[string]interface{}{
		"type":               "Ready",
		"status":             status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}

	payload := map[string]interface{}{
		"apiVersion": policy.APIVersion,
		"kind":       policy.Kind,
		"metadata": map[string]interface{}{
			"name":            policy.Metadata.Name,
			"namespace":       policy.Metadata.Namespace,
			"resourceVersion": policy.Metadata.ResourceVersion,
		},
		"status": map[string]interface{}{
			"observedGeneration": policy.Metadata.Generation,
			"conditions":         []map[string]interface{}{condition},
		},
	}

	respBody, updateStatus, err := client.doRequest("PUT", statusPath, payload)
	if err != nil {
		return err
	}
	if updateStatus < 200 || updateStatus >= 300 {
		if updateStatus == http.StatusConflict {
			latest, err := fetchRestorePolicy(client, policy.Metadata.Namespace, policy.Metadata.Name)
			if err != nil {
				return err
			}
			policy.Metadata.ResourceVersion = latest.Metadata.ResourceVersion
			payload["metadata"].(map[string]interface{})["resourceVersion"] = policy.Metadata.ResourceVersion
			respBody, updateStatus, err = client.doRequest("PUT", statusPath, payload)
			if err != nil {
				return err
			}
			if updateStatus >= 200 && updateStatus < 300 {
				return nil
			}
		}
		return fmt.Errorf("status update failed: %s status=%d body=%s", statusPath, updateStatus, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func fetchRestorePolicy(client *kubeClient, ns, name string) (RestorePolicy, error) {
	itemPath := namespacedPath(
		fmt.Sprintf("/apis/%s/%s", backupPolicyGroup, backupPolicyVersion),
		ns,
		"restorepolicies",
		name,
	)
	body, status, err := client.doRequest("GET", itemPath, nil)
	if err != nil {
		return RestorePolicy{}, err
	}
	if status != http.StatusOK {
		return RestorePolicy{}, fmt.Errorf("get failed: %s status=%d", itemPath, status)
	}
	var policy RestorePolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		return RestorePolicy{}, err
	}
	return policy, nil
}
