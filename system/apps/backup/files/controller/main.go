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
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type BackupPolicy struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		Annotations     map[string]string `json:"annotations,omitempty"`
		UID             string            `json:"uid"`
		ResourceVersion string            `json:"resourceVersion"`
		Generation      int64             `json:"generation"`
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
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		Annotations     map[string]string `json:"annotations,omitempty"`
		UID             string            `json:"uid"`
		ResourceVersion string            `json:"resourceVersion"`
		Generation      int64             `json:"generation"`
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

const processedHashAnnotation = "backup.homelab/processed-hash"

var reconcileHealthy atomic.Bool

type PolicyHandler interface {
	Reconcile(client *kubeClient, cfg Config) error
}

func main() {
	cfg := loadConfig()
	client, err := newKubeClient()
	if err != nil {
		panic(err)
	}

	go startHealthServer()

	if err := startInformers(client, cfg); err != nil {
		panic(err)
	}
}

func loadConfig() Config {
	return Config{
		ReconcileInterval:       mustDuration(getenv("RECONCILE_INTERVAL", "5m")),
		RepoPVCName:             getenv("REPO_PVC_NAME", "backup-repo"),
		RepoPVCSize:             getenv("REPO_PVC_SIZE", "100Gi"),
		RepoStorageClass:        getenv("REPO_STORAGE_CLASS", "nas-nfs-backup"),
		RepoMountPath:           strings.Trim(getenv("REPO_MOUNT_PATH", "restic-repo"), "/"),
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

	handlers := []PolicyHandler{
		&BackupPolicyHandler{},
		&RestorePolicyHandler{},
	}

	for _, handler := range handlers {
		if err := handler.Reconcile(client, cfg); err != nil {
			ok = false
		}
	}

	fmt.Printf("reconcile: completed in %s\n", time.Since(start).Truncate(time.Millisecond))
	reconcileHealthy.Store(ok)
}

func policySpecHash(spec interface{}) (string, error) {
	payload, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func updateProcessedHash(client *kubeClient, resource, ns, name, hash string) error {
	itemPath := namespacedPath(
		fmt.Sprintf("/apis/%s/%s", backupPolicyGroup, backupPolicyVersion),
		ns,
		resource,
		name,
	)

	payload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]interface{}{
				processedHashAnnotation: hash,
			},
		},
	}

	respBody, status, err := client.doRequestWithContentType("PATCH", itemPath, "application/merge-patch+json", payload)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("processed hash update failed: %s status=%d body=%s", itemPath, status, strings.TrimSpace(string(respBody)))
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
	return c.doRequestWithContentType(method, path, "application/json", body)
}

func (c *kubeClient) doRequestWithContentType(method, path, contentType string, body interface{}) ([]byte, int, error) {
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
		req.Header.Set("Content-Type", contentType)
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
