package main

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type BackupPolicy struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		UID             string `json:"uid"`
		ResourceVersion string `json:"resourceVersion"`
		Generation      int64  `json:"generation"`
	} `json:"metadata"`
	Spec   BackupPolicySpec   `json:"spec"`
	Status BackupPolicyStatus `json:"status,omitempty"`
}

type BackupPolicyList struct {
	Items []BackupPolicy `json:"items"`
}

type RestorePolicy struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		UID             string `json:"uid"`
		ResourceVersion string `json:"resourceVersion"`
		Generation      int64  `json:"generation"`
	} `json:"metadata"`
	Spec RestorePolicySpec `json:"spec"`
}

type RestorePolicyList struct {
	Items []RestorePolicy `json:"items"`
}

type BackupPolicySpec struct {
	Schedule string `json:"schedule"`
	TimeZone string `json:"timeZone,omitempty"`
	Volumes  []struct {
		PVC string `json:"pvc"`
	} `json:"volumes"`
	Quiesce *struct {
		ScaleDown []struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"scaleDown"`
	} `json:"quiesce,omitempty"`
	Export *struct {
		JobRef *struct {
			Name string `json:"name"`
		} `json:"jobRef"`
	} `json:"export,omitempty"`
}

type BackupPolicyStatus struct {
	LastSnapshotSync string                     `json:"lastSnapshotSync,omitempty"`
	Volumes          []BackupPolicyVolumeStatus `json:"volumes,omitempty"`
}

type BackupPolicyVolumeStatus struct {
	PVC       string           `json:"pvc"`
	LastSync  string           `json:"lastSync,omitempty"`
	Snapshots []BackupSnapshot `json:"snapshots,omitempty"`
}

type BackupSnapshot struct {
	ID      string `json:"id"`
	Time    string `json:"time"`
	Size    uint64 `json:"size"`
	Snippet string `json:"snippet"`
}

type RestorePolicySpec struct {
	SourceNamespace string `json:"sourceNamespace"`
	Volumes         []struct {
		SourcePVC   string `json:"sourcePVC"`
		TargetPVC   string `json:"targetPVC"`
		RestoreAsOf string `json:"restoreAsOf,omitempty"`
	} `json:"volumes"`
}

type Config struct {
	ReconcileInterval       time.Duration
	RepoPVCName             string
	RepoPVCSize             string
	RepoStorageClass        string
	RepoMountPath           string
	NFSEnabled              bool
	NFSServer               string
	NFSPath                 string
	PruneIntervalDays       int64
	RetainHourly            int64
	RetainDaily             int64
	RetainWeekly            int64
	RetainMonthly           int64
	RetainYearly            int64
	ExternalSecretStoreName string
	ExternalSecretStoreKind string
	ExternalSecretKey       string
	ResticPasswordProperty  string
	ResticS3BucketProperty  string
	ResticS3AccessKeyProp   string
	ResticS3SecretKeyProp   string
	RunnerImage             string
	RunnerImagePullPolicy   string
	ResticImage             string
	ScaleDownTimeoutSeconds int64
	ExportTimeoutSeconds    int64
	BackupTimeoutSeconds    int64
	OffsiteEnabled          bool
	OffsiteSchedule         string
	OffsiteTimeZone         string
}

const (
	backupPolicyGroup   = "backup.homelab"
	backupPolicyVersion = "v1alpha1"
)

var reconcileHealthy atomic.Bool

func main() {
	cfg := loadConfig()
	client, err := newKubeClient()
	if err != nil {
		panic(err)
	}

	go startHealthServer()

	for {
		reconcile(client, cfg)
		time.Sleep(cfg.ReconcileInterval)
	}
}

func loadConfig() Config {
	return Config{
		ReconcileInterval:       mustDuration(getenv("RECONCILE_INTERVAL", "5m")),
		RepoPVCName:             getenv("REPO_PVC_NAME", "backup-repo"),
		RepoPVCSize:             getenv("REPO_PVC_SIZE", "100Gi"),
		RepoStorageClass:        getenv("REPO_STORAGE_CLASS", "nas-nfs-backup"),
		RepoMountPath:           strings.Trim(getenv("REPO_MOUNT_PATH", "restic-repo"), "/"),
		NFSEnabled:              getenv("NFS_ENABLED", "false") == "true",
		NFSServer:               getenv("NFS_SERVER", ""),
		NFSPath:                 getenv("NFS_PATH", ""),
		PruneIntervalDays:       mustInt64(getenv("PRUNE_INTERVAL_DAYS", "14")),
		RetainHourly:            mustInt64(getenv("RETAIN_HOURLY", "6")),
		RetainDaily:             mustInt64(getenv("RETAIN_DAILY", "5")),
		RetainWeekly:            mustInt64(getenv("RETAIN_WEEKLY", "4")),
		RetainMonthly:           mustInt64(getenv("RETAIN_MONTHLY", "2")),
		RetainYearly:            mustInt64(getenv("RETAIN_YEARLY", "1")),
		ExternalSecretStoreName: getenv("EXTERNAL_SECRET_STORE_NAME", "global-secrets"),
		ExternalSecretStoreKind: getenv("EXTERNAL_SECRET_STORE_KIND", "ClusterSecretStore"),
		ExternalSecretKey:       getenv("EXTERNAL_SECRET_KEY", "external"),
		ResticPasswordProperty:  getenv("RESTIC_PASSWORD_PROPERTY", "restic-password"),
		ResticS3BucketProperty:  getenv("RESTIC_S3_BUCKET_PROPERTY", "restic-s3-bucket"),
		ResticS3AccessKeyProp:   getenv("RESTIC_S3_ACCESS_KEY_PROPERTY", "restic-s3-access-key"),
		ResticS3SecretKeyProp:   getenv("RESTIC_S3_SECRET_KEY_PROPERTY", "restic-s3-secret-key"),
		RunnerImage:             getenv("RUNNER_IMAGE", "bitnami/kubectl:latest"),
		RunnerImagePullPolicy:   getenv("RUNNER_IMAGE_PULL_POLICY", "IfNotPresent"),
		ResticImage:             getenv("RESTIC_IMAGE", "restic/restic:0.18.0"),
		ScaleDownTimeoutSeconds: mustInt64(getenv("SCALE_DOWN_TIMEOUT_SECONDS", "600")),
		ExportTimeoutSeconds:    mustInt64(getenv("EXPORT_TIMEOUT_SECONDS", "3600")),
		BackupTimeoutSeconds:    mustInt64(getenv("BACKUP_TIMEOUT_SECONDS", "7200")),
		OffsiteEnabled:          getenv("OFFSITE_ENABLED", "false") == "true",
		OffsiteSchedule:         getenv("OFFSITE_SCHEDULE", "0 3 * * 0"),
		OffsiteTimeZone:         getenv("OFFSITE_TIME_ZONE", "UTC"),
	}
}

func reconcile(client *kubeClient, cfg Config) {
	start := time.Now()
	fmt.Println("reconcile: starting")
	ok := true
	listPath := fmt.Sprintf("/apis/%s/%s/backuppolicies", backupPolicyGroup, backupPolicyVersion)
	body, status, err := client.doRequest("GET", listPath, nil)
	if err != nil {
		fmt.Printf("failed to list BackupPolicies: %v\n", err)
		ok = false
		reconcileHealthy.Store(ok)
		return
	}
	if status != http.StatusOK {
		fmt.Printf("failed to list BackupPolicies: status=%d\n", status)
		ok = false
		reconcileHealthy.Store(ok)
		return
	}

	var list BackupPolicyList
	if err := json.Unmarshal(body, &list); err != nil {
		fmt.Printf("failed to parse BackupPolicies: %v\n", err)
		ok = false
		reconcileHealthy.Store(ok)
		return
	}
	fmt.Printf("reconcile: found %d BackupPolicies\n", len(list.Items))

	for _, policy := range list.Items {
		volStatus, lastSnapshotSync, err := reconcilePolicy(client, cfg, policy)
		if err != nil {
			fmt.Printf("reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			ok = false
			if err := updateBackupPolicyStatus(client, policy, "False", "ReconcileError", err.Error(), volStatus, lastSnapshotSync); err != nil {
				fmt.Printf("status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				ok = false
			}
		} else {
			if err := updateBackupPolicyStatus(client, policy, "True", "Reconciled", "Reconcile successful", volStatus, lastSnapshotSync); err != nil {
				fmt.Printf("status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				ok = false
			}
		}
	}

	restoreListPath := fmt.Sprintf("/apis/%s/%s/restorepolicies", backupPolicyGroup, backupPolicyVersion)
	restoreBody, restoreStatus, restoreErr := client.doRequest("GET", restoreListPath, nil)
	if restoreErr != nil {
		fmt.Printf("failed to list RestorePolicies: %v\n", restoreErr)
		ok = false
		reconcileHealthy.Store(ok)
		return
	}
	if restoreStatus != http.StatusOK {
		fmt.Printf("failed to list RestorePolicies: status=%d\n", restoreStatus)
		ok = false
		reconcileHealthy.Store(ok)
		return
	}

	var restoreList RestorePolicyList
	if err := json.Unmarshal(restoreBody, &restoreList); err != nil {
		fmt.Printf("failed to parse RestorePolicies: %v\n", err)
		ok = false
		reconcileHealthy.Store(ok)
		return
	}
	fmt.Printf("reconcile: found %d RestorePolicies\n", len(restoreList.Items))

	for _, policy := range restoreList.Items {
		if err := reconcileRestorePolicy(client, cfg, policy); err != nil {
			fmt.Printf("restore reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			ok = false
			if err := updateRestoreStatus(client, policy, "False", "ReconcileError", err.Error()); err != nil {
				fmt.Printf("restore status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				ok = false
			}
		} else {
			if err := updateRestoreStatus(client, policy, "True", "Reconciled", "Reconcile successful"); err != nil {
				fmt.Printf("restore status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				ok = false
			}
		}
	}
	fmt.Printf("reconcile: completed in %s\n", time.Since(start).Truncate(time.Millisecond))
	reconcileHealthy.Store(ok)
}

func reconcilePolicy(client *kubeClient, cfg Config, policy BackupPolicy) ([]BackupPolicyVolumeStatus, string, error) {
	ns := policy.Metadata.Namespace
	name := policy.Metadata.Name
	if policy.APIVersion == "" {
		policy.APIVersion = fmt.Sprintf("%s/%s", backupPolicyGroup, backupPolicyVersion)
	}
	if policy.Kind == "" {
		policy.Kind = "BackupPolicy"
	}

	if !cfg.NFSEnabled {
		if err := ensureRepoPVC(client, cfg, ns); err != nil {
			return nil, policy.Status.LastSnapshotSync, err
		}
	}

	if err := ensureRunnerRBAC(client, ns); err != nil {
		return nil, policy.Status.LastSnapshotSync, err
	}

	existingStatus := map[string]BackupPolicyVolumeStatus{}
	for _, vol := range policy.Status.Volumes {
		existingStatus[vol.PVC] = vol
	}

	primarySources := make([]string, 0, len(policy.Spec.Volumes))
	offsiteSources := make([]string, 0, len(policy.Spec.Volumes))
	volumeStatuses := make([]BackupPolicyVolumeStatus, 0, len(policy.Spec.Volumes))
	lastSnapshotSync := policy.Status.LastSnapshotSync
	snapshotsUpdated := false

	for _, vol := range policy.Spec.Volumes {
		if vol.PVC == "" {
			continue
		}
		baseName := sanitizeName(fmt.Sprintf("backup-%s-%s", name, vol.PVC))
		secretName := sanitizeName(fmt.Sprintf("backup-repo-%s-%s", name, vol.PVC))

		if err := ensureExternalSecret(client, cfg, ns, secretName, vol.PVC, false, policy); err != nil {
			return volumeStatuses, lastSnapshotSync, err
		}
		if err := ensureReplicationSource(client, cfg, ns, baseName, secretName, vol.PVC, policy, true); err != nil {
			return volumeStatuses, lastSnapshotSync, err
		}
		primarySources = append(primarySources, baseName)

		statusEntry := BackupPolicyVolumeStatus{PVC: vol.PVC}
		existingEntry, hasExisting := existingStatus[vol.PVC]
		if hasExisting {
			statusEntry.Snapshots = existingEntry.Snapshots
			statusEntry.LastSync = existingEntry.LastSync
		}

		result, endTime, err := getReplicationSourceStatus(client, ns, baseName)
		if err != nil {
			return volumeStatuses, lastSnapshotSync, err
		}
		if endTime != "" {
			statusEntry.LastSync = normalizeTime(endTime)
		}

		if result == "Successful" && endTime != "" {
			if !hasExisting || existingEntry.LastSync != statusEntry.LastSync || len(existingEntry.Snapshots) == 0 {
				snapshots, err := fetchSnapshots(client, cfg, ns, policy.Metadata.Name, vol.PVC, secretName)
				if err != nil {
					return volumeStatuses, lastSnapshotSync, err
				}
				statusEntry.Snapshots = snapshots
				snapshotsUpdated = true
			}
		}

		volumeStatuses = append(volumeStatuses, statusEntry)

		if cfg.OffsiteEnabled {
			offsiteName := sanitizeName(fmt.Sprintf("backup-offsite-%s-%s", name, vol.PVC))
			offsiteSecret := sanitizeName(fmt.Sprintf("backup-repo-offsite-%s-%s", name, vol.PVC))
			if err := ensureExternalSecret(client, cfg, ns, offsiteSecret, vol.PVC, true, policy); err != nil {
				return volumeStatuses, lastSnapshotSync, err
			}
			if err := ensureReplicationSource(client, cfg, ns, offsiteName, offsiteSecret, vol.PVC, policy, false); err != nil {
				return volumeStatuses, lastSnapshotSync, err
			}
			offsiteSources = append(offsiteSources, offsiteName)
		}
	}

	if err := ensureCronJob(client, cfg, ns, policy, primarySources, false); err != nil {
		return volumeStatuses, lastSnapshotSync, err
	}
	if cfg.OffsiteEnabled {
		if err := ensureCronJob(client, cfg, ns, policy, offsiteSources, true); err != nil {
			return volumeStatuses, lastSnapshotSync, err
		}
	}

	if snapshotsUpdated {
		lastSnapshotSync = time.Now().UTC().Format(time.RFC3339)
	}

	return volumeStatuses, lastSnapshotSync, nil
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

func ensureRepoPVC(client *kubeClient, cfg Config, ns string) error {
	pvc := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]interface{}{
			"name":      cfg.RepoPVCName,
			"namespace": ns,
		},
		"spec": map[string]interface{}{
			"accessModes": []string{"ReadWriteMany"},
			"resources": map[string]interface{}{
				"requests": map[string]interface{}{
					"storage": cfg.RepoPVCSize,
				},
			},
			"storageClassName": cfg.RepoStorageClass,
		},
	}

	itemPath := namespacedPath("/api/v1", ns, "persistentvolumeclaims", cfg.RepoPVCName)
	collectionPath := namespacedPath("/api/v1", ns, "persistentvolumeclaims")
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
	_, createStatus, err := client.doRequest("POST", collectionPath, pvc)
	if err != nil {
		return err
	}
	if createStatus < 200 || createStatus >= 300 {
		return fmt.Errorf("create failed: %s status=%d", collectionPath, createStatus)
	}
	return nil
}

func ensureRunnerRBAC(client *kubeClient, ns string) error {
	sa := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ServiceAccount",
		"metadata": map[string]interface{}{
			"name":      "backup-runner",
			"namespace": ns,
		},
	}
	if err := client.upsert(namespacedPath("/api/v1", ns, "serviceaccounts", "backup-runner"),
		namespacedPath("/api/v1", ns, "serviceaccounts"), sa, nil); err != nil {
		return err
	}

	role := map[string]interface{}{
		"apiVersion": "rbac.authorization.k8s.io/v1",
		"kind":       "Role",
		"metadata": map[string]interface{}{
			"name":      "backup-runner",
			"namespace": ns,
		},
		"rules": []map[string]interface{}{
			{
				"apiGroups": []string{"apps"},
				"resources": []string{"deployments", "statefulsets", "deployments/scale", "statefulsets/scale"},
				"verbs":     []string{"get", "list", "watch", "patch", "update"},
			},
			{
				"apiGroups": []string{"batch"},
				"resources": []string{"jobs", "cronjobs"},
				"verbs":     []string{"get", "list", "watch", "create", "patch", "update"},
			},
			{
				"apiGroups": []string{"volsync.backube"},
				"resources": []string{"replicationsources"},
				"verbs":     []string{"get", "list", "watch", "patch", "update"},
			},
		},
	}
	if err := client.upsert(namespacedPath("/apis/rbac.authorization.k8s.io/v1", ns, "roles", "backup-runner"),
		namespacedPath("/apis/rbac.authorization.k8s.io/v1", ns, "roles"), role, nil); err != nil {
		return err
	}

	binding := map[string]interface{}{
		"apiVersion": "rbac.authorization.k8s.io/v1",
		"kind":       "RoleBinding",
		"metadata": map[string]interface{}{
			"name":      "backup-runner",
			"namespace": ns,
		},
		"subjects": []map[string]interface{}{
			{
				"kind":      "ServiceAccount",
				"name":      "backup-runner",
				"namespace": ns,
			},
		},
		"roleRef": map[string]interface{}{
			"kind":     "Role",
			"name":     "backup-runner",
			"apiGroup": "rbac.authorization.k8s.io",
		},
	}

	return client.upsert(namespacedPath("/apis/rbac.authorization.k8s.io/v1", ns, "rolebindings", "backup-runner"),
		namespacedPath("/apis/rbac.authorization.k8s.io/v1", ns, "rolebindings"), binding, nil)
}

func ensureExternalSecret(client *kubeClient, cfg Config, ns, secretName, pvc string, offsite bool, policy BackupPolicy) error {
	secretData := []map[string]interface{}{
		{
			"remoteRef": map[string]interface{}{"key": cfg.ExternalSecretKey, "property": cfg.ResticPasswordProperty},
			"secretKey": "restic_password",
		},
	}

	templateData := map[string]interface{}{
		"RESTIC_PASSWORD": "{{ .restic_password }}",
	}

	if offsite {
		secretData = append(secretData,
			map[string]interface{}{
				"remoteRef": map[string]interface{}{"key": cfg.ExternalSecretKey, "property": cfg.ResticS3BucketProperty},
				"secretKey": "restic_s3_bucket",
			},
			map[string]interface{}{
				"remoteRef": map[string]interface{}{"key": cfg.ExternalSecretKey, "property": cfg.ResticS3AccessKeyProp},
				"secretKey": "restic_s3_access_key",
			},
			map[string]interface{}{
				"remoteRef": map[string]interface{}{"key": cfg.ExternalSecretKey, "property": cfg.ResticS3SecretKeyProp},
				"secretKey": "restic_s3_secret_key",
			},
		)
		templateData["RESTIC_REPOSITORY"] = fmt.Sprintf("s3:{{{{ .restic_s3_bucket }}}}/%s/%s", ns, pvc)
		templateData["AWS_ACCESS_KEY_ID"] = "{{ .restic_s3_access_key }}"
		templateData["AWS_SECRET_ACCESS_KEY"] = "{{ .restic_s3_secret_key }}"
	} else {
		repoPath := fmt.Sprintf("/mnt/%s/%s/%s", cfg.RepoMountPath, ns, pvc)
		templateData["RESTIC_REPOSITORY"] = repoPath
	}

	obj := map[string]interface{}{
		"apiVersion": "external-secrets.io/v1beta1",
		"kind":       "ExternalSecret",
		"metadata": map[string]interface{}{
			"name":      secretName,
			"namespace": ns,
			"labels": map[string]interface{}{
				"backup-policy/name":      policy.Metadata.Name,
				"backup-policy/namespace": ns,
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
		namespacedPath("/apis/external-secrets.io/v1beta1", ns, "externalsecrets"), obj, &policy)
}

func ensureReplicationSource(client *kubeClient, cfg Config, ns, name, secretName, pvc string, policy BackupPolicy, useMover bool) error {
	resticSpec := map[string]interface{}{
		"repository":        secretName,
		"copyMethod":        "Snapshot",
		"pruneIntervalDays": cfg.PruneIntervalDays,
		"retain": map[string]interface{}{
			"hourly":  cfg.RetainHourly,
			"daily":   cfg.RetainDaily,
			"weekly":  cfg.RetainWeekly,
			"monthly": cfg.RetainMonthly,
			"yearly":  cfg.RetainYearly,
		},
	}

	if useMover {
		mountPath := fmt.Sprintf("/mnt/%s", cfg.RepoMountPath)
		if cfg.NFSEnabled {
			resticSpec["moverVolumes"] = []map[string]interface{}{
				{
					"mountPath": mountPath,
					"volumeSource": map[string]interface{}{
						"nfs": map[string]interface{}{
							"server": cfg.NFSServer,
							"path":   cfg.NFSPath,
						},
					},
				},
			}
		} else {
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
		}
	}

	obj := map[string]interface{}{
		"apiVersion": "volsync.backube/v1alpha1",
		"kind":       "ReplicationSource",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ns,
			"labels": map[string]interface{}{
				"backup-policy/name":      policy.Metadata.Name,
				"backup-policy/namespace": ns,
			},
		},
		"spec": map[string]interface{}{
			"sourcePVC": pvc,
			"trigger": map[string]interface{}{
				"manual": "init",
			},
			"restic": resticSpec,
		},
	}

	return client.upsert(namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationsources", name),
		namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationsources"), obj, &policy)
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
	mountPath := fmt.Sprintf("/mnt/%s", cfg.RepoMountPath)
	if cfg.NFSEnabled {
		resticSpec["moverVolumes"] = []map[string]interface{}{
			{
				"mountPath": mountPath,
				"volumeSource": map[string]interface{}{
					"nfs": map[string]interface{}{
						"server": cfg.NFSServer,
						"path":   cfg.NFSPath,
					},
				},
			},
		}
	} else {
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

func ensureCronJob(client *kubeClient, cfg Config, ns string, policy BackupPolicy, sources []string, offsite bool) error {
	if len(sources) == 0 {
		return nil
	}

	jobName := sanitizeName(fmt.Sprintf("backup-%s", policy.Metadata.Name))
	schedule := policy.Spec.Schedule
	timeZone := policy.Spec.TimeZone
	if offsite {
		jobName = sanitizeName(fmt.Sprintf("backup-%s-offsite", policy.Metadata.Name))
		schedule = cfg.OffsiteSchedule
		timeZone = cfg.OffsiteTimeZone
	}

	scaleTargets := []string{}
	if policy.Spec.Quiesce != nil {
		for _, target := range policy.Spec.Quiesce.ScaleDown {
			if target.Kind == "" || target.Name == "" {
				continue
			}
			kind := strings.ToLower(target.Kind)
			scaleTargets = append(scaleTargets, fmt.Sprintf("%s/%s", kind, target.Name))
		}
	}
	sort.Strings(scaleTargets)

	exportJob := ""
	if policy.Spec.Export != nil && policy.Spec.Export.JobRef != nil {
		exportJob = policy.Spec.Export.JobRef.Name
	}

	cron := map[string]interface{}{
		"apiVersion": "batch/v1",
		"kind":       "CronJob",
		"metadata": map[string]interface{}{
			"name":      jobName,
			"namespace": ns,
			"labels": map[string]interface{}{
				"backup-policy/name":      policy.Metadata.Name,
				"backup-policy/namespace": ns,
			},
			"annotations": map[string]interface{}{
				"backup-script-hash": backupScriptHash(),
			},
		},
		"spec": map[string]interface{}{
			"schedule":                   schedule,
			"concurrencyPolicy":          "Forbid",
			"successfulJobsHistoryLimit": 2,
			"failedJobsHistoryLimit":     2,
			"jobTemplate": map[string]interface{}{
				"spec": map[string]interface{}{
					"template": map[string]interface{}{
						"spec": map[string]interface{}{
							"serviceAccountName": "backup-runner",
							"restartPolicy":      "Never",
							"containers": []map[string]interface{}{
								{
									"name":            "backup",
									"image":           cfg.RunnerImage,
									"imagePullPolicy": cfg.RunnerImagePullPolicy,
									"command":         []string{"/bin/sh", "-c"},
									"args":            []string{backupScript()},
									"env": []map[string]interface{}{
										{"name": "NAMESPACE", "value": ns},
										{"name": "SCALE_DOWN_TARGETS", "value": strings.Join(scaleTargets, " ")},
										{"name": "EXPORT_JOB_NAME", "value": exportJob},
										{"name": "REPLICATION_SOURCES", "value": strings.Join(sources, " ")},
										{"name": "SCALE_DOWN_TIMEOUT_SECONDS", "value": fmt.Sprintf("%d", cfg.ScaleDownTimeoutSeconds)},
										{"name": "EXPORT_TIMEOUT_SECONDS", "value": fmt.Sprintf("%d", cfg.ExportTimeoutSeconds)},
										{"name": "BACKUP_TIMEOUT_SECONDS", "value": fmt.Sprintf("%d", cfg.BackupTimeoutSeconds)},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if timeZone != "" {
		cronSpec := cron["spec"].(map[string]interface{})
		cronSpec["timeZone"] = timeZone
	}

	return client.upsert(namespacedPath("/apis/batch/v1", ns, "cronjobs", jobName),
		namespacedPath("/apis/batch/v1", ns, "cronjobs"), cron, &policy)
}

func backupScript() string {
	return strings.TrimSpace(`
set -euo pipefail

scaled_file="$(mktemp)"

cleanup() {
  if [ -s "${scaled_file}" ]; then
    while read -r target replicas; do
      if [ -n "${target}" ] && [ -n "${replicas}" ]; then
        kubectl -n "${NAMESPACE}" scale "${target}" --replicas="${replicas}" >/dev/null 2>&1 || true
      fi
    done < "${scaled_file}"
  fi
}

on_error() {
  echo "Backup failed, restoring scaled workloads..." >&2
  cleanup
}

trap on_error ERR
trap cleanup EXIT

if [ -n "${SCALE_DOWN_TARGETS:-}" ]; then
  for target in ${SCALE_DOWN_TARGETS}; do
    replicas="$(kubectl -n "${NAMESPACE}" get "${target}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
    if [ -z "${replicas}" ]; then
      echo "Failed to read replicas for ${target}"
      exit 1
    fi
    echo "${target} ${replicas}" >> "${scaled_file}"
    kubectl -n "${NAMESPACE}" scale "${target}" --replicas=0
  done
  for target in ${SCALE_DOWN_TARGETS}; do
    kubectl -n "${NAMESPACE}" rollout status "${target}" --timeout="${SCALE_DOWN_TIMEOUT_SECONDS}s"
  done
fi

if [ -n "${EXPORT_JOB_NAME:-}" ]; then
  job_name="${EXPORT_JOB_NAME}-run-$(date -u +%Y%m%d%H%M%S)"
  kubectl -n "${NAMESPACE}" create job "${job_name}" --from="cronjob/${EXPORT_JOB_NAME}"
  if ! kubectl -n "${NAMESPACE}" wait --for=condition=complete "job/${job_name}" --timeout="${EXPORT_TIMEOUT_SECONDS}s"; then
    kubectl -n "${NAMESPACE}" logs "job/${job_name}" || true
    exit 1
  fi
fi

trigger_id="$(date -u +%Y%m%d%H%M%S)"
for source in ${REPLICATION_SOURCES}; do
  deadline="$(( $(date +%s) + 60 ))"
  while true; do
    if kubectl -n "${NAMESPACE}" get replicationsource "${source}" >/dev/null 2>&1; then
      break
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "ReplicationSource ${source} not found before trigger."
      exit 1
    fi
    sleep 2
  done
done
for source in ${REPLICATION_SOURCES}; do
  kubectl -n "${NAMESPACE}" patch replicationsource "${source}" --type merge -p "{\"spec\":{\"trigger\":{\"manual\":\"${trigger_id}\"}}}"
done

for source in ${REPLICATION_SOURCES}; do
  deadline="$(( $(date +%s) + ${BACKUP_TIMEOUT_SECONDS} ))"
  while true; do
    last_manual="$(kubectl -n "${NAMESPACE}" get replicationsource "${source}" -o jsonpath='{.status.lastManualSync}' 2>/dev/null || true)"
    result="$(kubectl -n "${NAMESPACE}" get replicationsource "${source}" -o jsonpath='{.status.latestMoverStatus.result}' 2>/dev/null || true)"

    if [ "${last_manual}" = "${trigger_id}" ] && [ -n "${result}" ]; then
      if [ "${result}" = "Successful" ]; then
        echo "ReplicationSource ${source} completed successfully."
        break
      fi
      echo "ReplicationSource ${source} failed (result=${result})."
      exit 1
    fi

    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "Timed out waiting for ReplicationSource ${source}."
      exit 1
    fi

    sleep 10
  done
 done
`)
}

func backupScriptHash() string {
	sum := sha256.Sum256([]byte(backupScript()))
	return hex.EncodeToString(sum[:])
}

func getReplicationSourceStatus(client *kubeClient, ns, name string) (string, string, error) {
	itemPath := namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationsources", name)
	body, status, err := client.doRequest("GET", itemPath, nil)
	if err != nil {
		return "", "", err
	}
	if status == http.StatusNotFound {
		return "", "", nil
	}
	if status != http.StatusOK {
		return "", "", fmt.Errorf("get failed: %s status=%d", itemPath, status)
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		return "", "", err
	}
	statusMap, _ := obj["status"].(map[string]interface{})
	if statusMap == nil {
		return "", "", nil
	}
	latestMover, _ := statusMap["latestMoverStatus"].(map[string]interface{})
	result, _ := latestMover["result"].(string)
	endTime, _ := latestMover["endTime"].(string)
	if endTime == "" {
		startTime, _ := latestMover["startTime"].(string)
		if startTime == "" {
			startTime, _ = latestMover["startTimestamp"].(string)
		}
		if startTime != "" {
			endTime = startTime
		}
	}
	if endTime == "" {
		manual, _ := statusMap["lastManualSync"].(string)
		endTime = manual
	}
	return result, endTime, nil
}

func normalizeTime(value string) string {
	if value == "" {
		return ""
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC().Format(time.RFC3339)
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC().Format(time.RFC3339)
	}
	if parsed, err := time.Parse("20060102150405", value); err == nil {
		return parsed.UTC().Format(time.RFC3339)
	}
	return ""
}

func fetchSnapshots(client *kubeClient, cfg Config, ns, policyName, pvc, secretName string) ([]BackupSnapshot, error) {
	jobName := sanitizeName(fmt.Sprintf("backup-snapshots-%s-%s-%d", policyName, pvc, time.Now().UTC().Unix()))
	if err := ensureSnapshotJob(client, cfg, ns, jobName, secretName); err != nil {
		return nil, err
	}
	defer func() {
		_ = deleteJob(client, ns, jobName)
	}()

	if err := waitForJobCompletion(client, ns, jobName, 5*time.Minute); err != nil {
		return nil, err
	}

	logs, err := getJobLogs(client, ns, jobName)
	if err != nil {
		return nil, err
	}

	var raw []map[string]interface{}
	if err := json.Unmarshal([]byte(logs), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse restic snapshots output: %w", err)
	}

	snapshots := make([]BackupSnapshot, 0, len(raw))
	for _, item := range raw {
		id, _ := item["id"].(string)
		timeVal, _ := item["time"].(string)
		var size uint64
		if sizeVal, ok := item["size"].(float64); ok {
			if sizeVal > 0 {
				size = uint64(sizeVal)
			}
		}
		if id == "" || timeVal == "" {
			continue
		}
		snapshots = append(snapshots, BackupSnapshot{
			ID:      id,
			Time:    timeVal,
			Size:    size,
			Snippet: fmt.Sprintf("restoreAsOf: \"%s\"  # %s", timeVal, id),
		})
	}
	return snapshots, nil
}

func ensureSnapshotJob(client *kubeClient, cfg Config, ns, jobName, secretName string) error {
	mountPath := fmt.Sprintf("/mnt/%s", cfg.RepoMountPath)
	container := map[string]interface{}{
		"name":            "restic",
		"image":           cfg.ResticImage,
		"imagePullPolicy": "IfNotPresent",
		"envFrom": []map[string]interface{}{
			{
				"secretRef": map[string]interface{}{
					"name": secretName,
				},
			},
		},
		"command": []string{"/bin/sh", "-c"},
		"args":    []string{"restic snapshots --json"},
		"volumeMounts": []map[string]interface{}{
			{
				"name":      "repo",
				"mountPath": mountPath,
			},
		},
	}

	volumes := []map[string]interface{}{
		{
			"name": "repo",
		},
	}

	if cfg.NFSEnabled {
		volumes[0]["nfs"] = map[string]interface{}{
			"server": cfg.NFSServer,
			"path":   cfg.NFSPath,
		}
	} else {
		volumes[0]["persistentVolumeClaim"] = map[string]interface{}{
			"claimName": cfg.RepoPVCName,
			"readOnly":  false,
		}
	}

	job := map[string]interface{}{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]interface{}{
			"name":      jobName,
			"namespace": ns,
		},
		"spec": map[string]interface{}{
			"backoffLimit": 0,
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"serviceAccountName": "backup-runner",
					"restartPolicy":      "Never",
					"containers":         []map[string]interface{}{container},
					"volumes":            volumes,
				},
			},
		},
	}

	return client.upsert(namespacedPath("/apis/batch/v1", ns, "jobs", jobName),
		namespacedPath("/apis/batch/v1", ns, "jobs"), job, nil)
}

func waitForJobCompletion(client *kubeClient, ns, jobName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		itemPath := namespacedPath("/apis/batch/v1", ns, "jobs", jobName)
		body, status, err := client.doRequest("GET", itemPath, nil)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("get failed: %s status=%d", itemPath, status)
		}
		var job map[string]interface{}
		if err := json.Unmarshal(body, &job); err != nil {
			return err
		}
		statusObj, _ := job["status"].(map[string]interface{})
		if statusObj != nil {
			if succeeded, ok := statusObj["succeeded"].(float64); ok && succeeded > 0 {
				return nil
			}
			if failed, ok := statusObj["failed"].(float64); ok && failed > 0 {
				return fmt.Errorf("job %s failed", jobName)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for job %s", jobName)
		}
		time.Sleep(2 * time.Second)
	}
}

func getJobLogs(client *kubeClient, ns, jobName string) (string, error) {
	selector := url.QueryEscape(fmt.Sprintf("job-name=%s", jobName))
	listPath := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=%s", ns, selector)
	body, status, err := client.doRequest("GET", listPath, nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("get failed: %s status=%d", listPath, status)
	}
	var podList map[string]interface{}
	if err := json.Unmarshal(body, &podList); err != nil {
		return "", err
	}
	items, _ := podList["items"].([]interface{})
	if len(items) == 0 {
		return "", fmt.Errorf("no pods found for job %s", jobName)
	}
	pod, _ := items[0].(map[string]interface{})
	meta, _ := pod["metadata"].(map[string]interface{})
	podName, _ := meta["name"].(string)
	if podName == "" {
		return "", fmt.Errorf("pod name not found for job %s", jobName)
	}
	logPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log", ns, podName)
	logBody, logStatus, err := client.doRequest("GET", logPath, nil)
	if err != nil {
		return "", err
	}
	if logStatus != http.StatusOK {
		return "", fmt.Errorf("get failed: %s status=%d", logPath, logStatus)
	}
	return string(logBody), nil
}

func deleteJob(client *kubeClient, ns, jobName string) error {
	itemPath := namespacedPath("/apis/batch/v1", ns, "jobs", jobName)
	_, status, err := client.doRequest("DELETE", itemPath, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("delete failed: %s status=%d", itemPath, status)
	}
	return nil
}

type kubeClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func newKubeClient() (*kubeClient, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("missing KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT")
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool},
	}

	return &kubeClient{
		baseURL: fmt.Sprintf("https://%s:%s", host, port),
		token:   strings.TrimSpace(string(token)),
		client:  &http.Client{Transport: transport, Timeout: 30 * time.Second},
	}, nil
}

func (c *kubeClient) doRequest(method, path string, body interface{}) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, c.baseURL+path, reader)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	return respBody, resp.StatusCode, nil
}

func (c *kubeClient) upsert(itemPath, collectionPath string, obj map[string]interface{}, owner *BackupPolicy) error {
	body, status, err := c.doRequest("GET", itemPath, nil)
	if err != nil {
		return err
	}

	if status == http.StatusNotFound {
		if owner != nil {
			setOwnerRef(obj, owner)
		}
		_, createStatus, err := c.doRequest("POST", collectionPath, obj)
		if err != nil {
			return err
		}
		if createStatus < 200 || createStatus >= 300 {
			return fmt.Errorf("create failed: %s status=%d", collectionPath, createStatus)
		}
		return nil
	}
	if status != http.StatusOK {
		return fmt.Errorf("get failed: %s status=%d", itemPath, status)
	}

	var existing map[string]interface{}
	if err := json.Unmarshal(body, &existing); err != nil {
		return err
	}

	metadata, _ := existing["metadata"].(map[string]interface{})
	resourceVersion, _ := metadata["resourceVersion"].(string)
	if resourceVersion != "" {
		objMetadata, _ := obj["metadata"].(map[string]interface{})
		if objMetadata == nil {
			objMetadata = map[string]interface{}{}
			obj["metadata"] = objMetadata
		}
		objMetadata["resourceVersion"] = resourceVersion
	}

	if owner != nil {
		setOwnerRef(obj, owner)
	}

	_, updateStatus, err := c.doRequest("PUT", itemPath, obj)
	if err != nil {
		return err
	}
	if updateStatus < 200 || updateStatus >= 300 {
		return fmt.Errorf("update failed: %s status=%d", itemPath, updateStatus)
	}
	return nil
}

func setOwnerRef(obj map[string]interface{}, owner *BackupPolicy) {
	metadata, _ := obj["metadata"].(map[string]interface{})
	if metadata == nil {
		metadata = map[string]interface{}{}
		obj["metadata"] = metadata
	}
	metadata["ownerReferences"] = []map[string]interface{}{
		{
			"apiVersion": owner.APIVersion,
			"kind":       owner.Kind,
			"name":       owner.Metadata.Name,
			"uid":        owner.Metadata.UID,
		},
	}
}

func namespacedPath(base, ns, resource string, name ...string) string {
	if len(name) == 0 || name[0] == "" {
		return fmt.Sprintf("%s/namespaces/%s/%s", base, ns, resource)
	}
	return fmt.Sprintf("%s/namespaces/%s/%s/%s", base, ns, resource, name[0])
}

func sanitizeName(value string) string {
	re := regexp.MustCompile(`[^a-z0-9-]`)
	value = strings.ToLower(value)
	value = re.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	if len(value) <= 253 {
		return value
	}
	hash := sha256.Sum256([]byte(value))
	suffix := hex.EncodeToString(hash[:6])
	trim := 253 - len(suffix) - 1
	return fmt.Sprintf("%s-%s", value[:trim], suffix)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func mustDuration(value string) time.Duration {
	dur, err := time.ParseDuration(value)
	if err != nil {
		panic(err)
	}
	return dur
}

func mustInt64(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		panic(err)
	}
	return parsed
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
		return fmt.Errorf("status update failed: %s status=%d body=%s", statusPath, updateStatus, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func updateBackupPolicyStatus(client *kubeClient, policy BackupPolicy, status, reason, message string, volumes []BackupPolicyVolumeStatus, lastSnapshotSync string) error {
	if policy.Metadata.Name == "" || policy.Metadata.Namespace == "" {
		return fmt.Errorf("missing policy name/namespace for status update")
	}
	statusPath := namespacedPath(
		fmt.Sprintf("/apis/%s/%s", backupPolicyGroup, backupPolicyVersion),
		policy.Metadata.Namespace,
		"backuppolicies",
		policy.Metadata.Name,
	) + "/status"

	condition := map[string]interface{}{
		"type":               "Ready",
		"status":             status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}

	statusMap := map[string]interface{}{
		"observedGeneration": policy.Metadata.Generation,
		"conditions":         []map[string]interface{}{condition},
		"volumes":            volumes,
	}
	if lastSnapshotSync != "" {
		statusMap["lastSnapshotSync"] = lastSnapshotSync
	}

	payload := map[string]interface{}{
		"apiVersion": policy.APIVersion,
		"kind":       policy.Kind,
		"metadata": map[string]interface{}{
			"name":            policy.Metadata.Name,
			"namespace":       policy.Metadata.Namespace,
			"resourceVersion": policy.Metadata.ResourceVersion,
		},
		"status": statusMap,
	}

	respBody, updateStatus, err := client.doRequest("PUT", statusPath, payload)
	if err != nil {
		return err
	}
	if updateStatus < 200 || updateStatus >= 300 {
		return fmt.Errorf("status update failed: %s status=%d body=%s", statusPath, updateStatus, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func startHealthServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if reconcileHealthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Println("health server starting on :8080")
	if err := server.ListenAndServe(); err != nil {
		fmt.Printf("health server stopped: %v\n", err)
	}
}
