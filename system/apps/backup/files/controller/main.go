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
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	Spec BackupPolicySpec `json:"spec"`
}

type BackupPolicyList struct {
	Items []BackupPolicy `json:"items"`
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

func main() {
	cfg := loadConfig()
	client, err := newKubeClient()
	if err != nil {
		panic(err)
	}

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
	listPath := fmt.Sprintf("/apis/%s/%s/backuppolicies", backupPolicyGroup, backupPolicyVersion)
	body, status, err := client.doRequest("GET", listPath, nil)
	if err != nil {
		fmt.Printf("failed to list BackupPolicies: %v\n", err)
		return
	}
	if status != http.StatusOK {
		fmt.Printf("failed to list BackupPolicies: status=%d\n", status)
		return
	}

	var list BackupPolicyList
	if err := json.Unmarshal(body, &list); err != nil {
		fmt.Printf("failed to parse BackupPolicies: %v\n", err)
		return
	}
	fmt.Printf("reconcile: found %d BackupPolicies\n", len(list.Items))

	for _, policy := range list.Items {
		if err := reconcilePolicy(client, cfg, policy); err != nil {
			fmt.Printf("reconcile failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			if err := updatePolicyStatus(client, policy, "False", "ReconcileError", err.Error()); err != nil {
				fmt.Printf("status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			}
		} else {
			if err := updatePolicyStatus(client, policy, "True", "Reconciled", "Reconcile successful"); err != nil {
				fmt.Printf("status update failed for %s/%s: %v\n", policy.Metadata.Namespace, policy.Metadata.Name, err)
			}
		}
	}
	fmt.Printf("reconcile: completed in %s\n", time.Since(start).Truncate(time.Millisecond))
}

func reconcilePolicy(client *kubeClient, cfg Config, policy BackupPolicy) error {
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
			return err
		}
	}

	if err := ensureRunnerRBAC(client, ns); err != nil {
		return err
	}

	primarySources := make([]string, 0, len(policy.Spec.Volumes))
	offsiteSources := make([]string, 0, len(policy.Spec.Volumes))

	for _, vol := range policy.Spec.Volumes {
		if vol.PVC == "" {
			continue
		}
		baseName := sanitizeName(fmt.Sprintf("backup-%s-%s", name, vol.PVC))
		secretName := sanitizeName(fmt.Sprintf("backup-repo-%s-%s", name, vol.PVC))

		if err := ensureExternalSecret(client, cfg, ns, secretName, vol.PVC, false, policy); err != nil {
			return err
		}
		if err := ensureReplicationSource(client, cfg, ns, baseName, secretName, vol.PVC, policy, true); err != nil {
			return err
		}
		primarySources = append(primarySources, baseName)

		if cfg.OffsiteEnabled {
			offsiteName := sanitizeName(fmt.Sprintf("backup-offsite-%s-%s", name, vol.PVC))
			offsiteSecret := sanitizeName(fmt.Sprintf("backup-repo-offsite-%s-%s", name, vol.PVC))
			if err := ensureExternalSecret(client, cfg, ns, offsiteSecret, vol.PVC, true, policy); err != nil {
				return err
			}
			if err := ensureReplicationSource(client, cfg, ns, offsiteName, offsiteSecret, vol.PVC, policy, false); err != nil {
				return err
			}
			offsiteSources = append(offsiteSources, offsiteName)
		}
	}

	if err := ensureCronJob(client, cfg, ns, policy, primarySources, false); err != nil {
		return err
	}
	if cfg.OffsiteEnabled {
		if err := ensureCronJob(client, cfg, ns, policy, offsiteSources, true); err != nil {
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
	body, status, err := client.doRequest("GET", itemPath, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		_ = body
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
		if cfg.NFSEnabled {
			resticSpec["moverVolumes"] = []map[string]interface{}{
				{
					"mountPath": cfg.RepoMountPath,
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
					"mountPath": cfg.RepoMountPath,
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

func updatePolicyStatus(client *kubeClient, policy BackupPolicy, status, reason, message string) error {
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

	_, updateStatus, err := client.doRequest("PUT", statusPath, payload)
	if err != nil {
		return err
	}
	if updateStatus < 200 || updateStatus >= 300 {
		return fmt.Errorf("status update failed: %s status=%d", statusPath, updateStatus)
	}
	return nil
}
