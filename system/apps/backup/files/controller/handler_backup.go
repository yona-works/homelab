package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type BackupPolicyHandler struct{}

func (h *BackupPolicyHandler) Reconcile(client *kubeClient, cfg Config) error {
	listPath := fmt.Sprintf("/apis/%s/%s/backuppolicies", backupPolicyGroup, backupPolicyVersion)
	body, status, err := client.doRequest("GET", listPath, nil)
	if err != nil {
		fmt.Printf("failed to list BackupPolicies: %v\n", err)
		return err
	}
	if status != http.StatusOK {
		err := fmt.Errorf("failed to list BackupPolicies: status=%d", status)
		fmt.Println(err.Error())
		return err
	}

	var list BackupPolicyList
	if err := json.Unmarshal(body, &list); err != nil {
		fmt.Printf("failed to parse BackupPolicies: %v\n", err)
		return err
	}
	fmt.Printf("reconcile: found %d BackupPolicies\n", len(list.Items))

	for _, policy := range list.Items {
		hash, err := policySpecHash(policy.Spec)
		if err != nil {
			fmt.Printf("reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			return err
		}
		if policy.Metadata.Annotations != nil && policy.Metadata.Annotations[processedHashAnnotation] == hash {
			continue
		}

		volStatus, lastSnapshotSync, err := reconcileBackupPolicy(client, cfg, policy)
		if err != nil {
			fmt.Printf("reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			if err := updateBackupPolicyStatus(client, policy, "False", "ReconcileError", err.Error(), volStatus, lastSnapshotSync); err != nil {
				fmt.Printf("status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				return err
			}
			if err := updateProcessedHash(client, "backuppolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
				fmt.Printf("processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
				return err
			}
			return err
		}
		if err := updateBackupPolicyStatus(client, policy, "True", "Reconciled", "Reconcile successful", volStatus, lastSnapshotSync); err != nil {
			fmt.Printf("status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			return err
		}
		if err := updateProcessedHash(client, "backuppolicies", policy.Metadata.Namespace, policy.Metadata.Name, hash); err != nil {
			fmt.Printf("processed hash update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			return err
		}
	}

	return nil
}

func reconcileBackupPolicy(client *kubeClient, cfg Config, policy BackupPolicy) ([]BackupPolicyVolumeStatus, string, error) {
	ns := policy.Metadata.Namespace
	name := policy.Metadata.Name
	if policy.APIVersion == "" {
		policy.APIVersion = fmt.Sprintf("%s/%s", backupPolicyGroup, backupPolicyVersion)
	}
	if policy.Kind == "" {
		policy.Kind = "BackupPolicy"
	}

	fmt.Printf("reconcile policy %s/%s: %d volumes\n", ns, name, len(policy.Spec.Volumes))

	if err := ensureRepoPVC(client, cfg, ns); err != nil {
		return nil, policy.Status.LastSnapshotSync, err
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
			fmt.Printf("reconcile policy %s/%s: skipping empty pvc entry\n", ns, name)
			continue
		}
		fmt.Printf("reconcile policy %s/%s: ensuring volume %s\n", ns, name, vol.PVC)
		baseName := sanitizeName(fmt.Sprintf("backup-%s-%s", name, vol.PVC))
		secretName := sanitizeName(fmt.Sprintf("backup-repo-%s-%s", name, vol.PVC))

		fmt.Printf("reconcile policy %s/%s: ensuring ExternalSecret %s\n", ns, name, secretName)
		if err := ensureExternalSecret(client, cfg, ns, secretName, vol.PVC, false, policy); err != nil {
			return volumeStatuses, lastSnapshotSync, err
		}
		fmt.Printf("reconcile policy %s/%s: ensuring ReplicationSource %s\n", ns, name, baseName)
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

	if err := client.upsert(namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationsources", name),
		namespacedPath("/apis/volsync.backube/v1alpha1", ns, "replicationsources"), obj, &policy); err != nil {
		return err
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

echo "Backup job starting in namespace ${NAMESPACE}"
echo "Replication sources: ${REPLICATION_SOURCES}"
echo "Scale down targets: ${SCALE_DOWN_TARGETS:-<none>}"
echo "Export job: ${EXPORT_JOB_NAME:-<none>}"

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
  echo "Waiting for ReplicationSource ${source} to exist..."
  deadline="$(( $(date +%s) + 300 ))"
  while true; do
    if kubectl -n "${NAMESPACE}" get replicationsource "${source}" >/dev/null 2>&1; then
      break
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "ReplicationSource ${source} not found before trigger."
      echo "Available ReplicationSources:"
      kubectl -n "${NAMESPACE}" get replicationsources -o name || true
      exit 1
    fi
    echo "ReplicationSource ${source} not found yet, retrying..."
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
			"persistentVolumeClaim": map[string]interface{}{
				"claimName": cfg.RepoPVCName,
				"readOnly":  false,
			},
		},
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
		if updateStatus == http.StatusConflict {
			latest, err := fetchBackupPolicy(client, policy.Metadata.Namespace, policy.Metadata.Name)
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

func fetchBackupPolicy(client *kubeClient, ns, name string) (BackupPolicy, error) {
	itemPath := namespacedPath(
		fmt.Sprintf("/apis/%s/%s", backupPolicyGroup, backupPolicyVersion),
		ns,
		"backuppolicies",
		name,
	)
	body, status, err := client.doRequest("GET", itemPath, nil)
	if err != nil {
		return BackupPolicy{}, err
	}
	if status != http.StatusOK {
		return BackupPolicy{}, fmt.Errorf("get failed: %s status=%d", itemPath, status)
	}
	var policy BackupPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		return BackupPolicy{}, err
	}
	return policy, nil
}
