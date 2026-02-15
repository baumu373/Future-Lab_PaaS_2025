package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// --- 環境変数ヘルパー ---
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// --- Keycloak設定 (環境変数対応) ---
var (
	// [変更] 環境変数 KEYCLOAK_URL から取得 (デフォルトは localhost)
	keycloakBaseUrl = getEnv("KEYCLOAK_URL", "http://localhost:8080")
	keycloakIssuer  = keycloakBaseUrl + "/realms/future-lab"
	keycloakClientID = "future-lab-frontend"
)

var (
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
)

// --- 外部アクセス用IP (環境変数対応) ---
// [新規] スマホ等からアクセスする場合、k8sが返す "localhost" ではなく、このIPを返す
var externalIP = getEnv("EXTERNAL_IP", "localhost")

// --- 管理用MinIO（スナップショット用）設定 ---
// [変更] MinIO設定も環境変数から取れるようにするとk3s移行時に楽です
var (
	snapshotEndpoint        = getEnv("MINIO_ENDPOINT", "127.0.0.1:31950") // k8s内部またはポートフォワード
	snapshotAccessKeyID     = getEnv("MINIO_ACCESS_KEY", "minioadmin")
	snapshotSecretAccessKey = getEnv("MINIO_SECRET_KEY", "minioadmin")
	snapshotUseSSL          = false
	snapshotBucketName      = "snapshots"
)

var (
	minioClient *minio.Client
)

// --- 案1：メタデータDB設定 ---
var (
	dbHost     = getEnv("DB_HOST", "localhost")
	dbPort     = 5433 // ポートはint変換が必要ですが簡略化のため固定または別途処理
	dbUser     = getEnv("DB_USER", "keycloak")
	dbPassword = getEnv("DB_PASSWORD", "password")
	dbName     = getEnv("DB_NAME", "keycloak")
)

var (
	dbPool *pgxpool.Pool
)

// --- 内部台帳（あるべき姿）の定義 ---
type ExpectedResources struct {
	UserID      string
	Namespace   string
	PackageName string
}

var internalLedger = &sync.Map{}

// --- Structs ---
type StartRequest struct {
	PackageName string `json:"packageName"`
}
type SQLRequest struct {
	SQLQuery string `json:"sqlQuery"`
}
type ExperimentResponse struct {
	Status             string         `json:"status"`
	Message            string         `json:"message"`
	ServicePorts       map[string]int `json:"servicePorts,omitempty"`
	PublicServicePorts map[string]int `json:"publicServicePorts,omitempty"`
	// [新規] クライアントが接続すべきホスト名/IPを返す
	HostIP             string         `json:"hostIP,omitempty"` 
}
type SQLResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message,omitempty"`
	Headers []string        `json:"headers,omitempty"`
	Rows    [][]interface{} `json:"rows,omitempty"`
}
type ActiveExperiment struct {
	UserID            string `json:"userId"`
	DeploymentName    string `json:"deploymentName"`
	PackageName       string `json:"packageName"`
	CreationTimestamp string `json:"creationTimestamp"`
	Namespace         string `json:"namespace"`
}
type AdminActionRequest struct {
	UserID string `json:"userId"`
}
type SnapshotCreateRequest struct {
	SnapshotName string `json:"snapshotName"`
	IsPublic     bool   `json:"isPublic"`
}
type SnapshotRestoreRequest struct {
	SnapshotName string `json:"snapshotName"`
}
type SnapshotInfo struct {
	Name          string    `json:"name"`
	DisplayName   string    `json:"displayName"`
	OwnerUserID   string    `json:"ownerUserID"`
	OwnerUsername string    `json:"ownerUsername,omitempty"` // ログイン名
	IsPublic      bool      `json:"isPublic"`
	LastModified  time.Time `json:"lastModified"`
}
type AdminDeleteSnapshotRequest struct {
	SafeName    string `json:"safeName"`
	OwnerUserID string `json:"ownerUserID"`
}

// --- JSONヘルパー関数 ---
func sendJSONResponse(w http.ResponseWriter, data interface{}, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}
func sendJSONError(w http.ResponseWriter, message string, code int) {
	sendJSONResponse(w, map[string]string{"message": message}, code)
}

// --- Kubernetes Utilities (変更なし) ---
func executeKubectlExecToFile(namespace, podName, containerName, localFilePath string, command ...string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("ホームディレクトリの取得に失敗: %w", err)
	}
	kubeconfigPath := filepath.Join(home, ".kube", "config")
	baseArgs := []string{"--kubeconfig", kubeconfigPath, "-n", namespace, "exec", podName, "-c", containerName, "--"}
	args := append(baseArgs, command...)
	cmd := exec.Command("kubectl", args...)
	outfile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("ローカルファイル '%s' の作成に失敗: %w", localFilePath, err)
	}
	defer outfile.Close()
	cmd.Stdout = outfile
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl exec (stdout to file) 失敗: %v, stderr: %s", err, stderr.String())
	}
	return nil
}
func executeKubectlExecStreamIn(namespace, podName, containerName, localFilePath string, command ...string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("ホームディレクトリの取得に失敗: %w", err)
	}
	kubeconfigPath := filepath.Join(home, ".kube", "config")
	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("ローカルファイル '%s' を開けません: %w", localFilePath, err)
	}
	defer file.Close()
	baseArgs := []string{"--kubeconfig", kubeconfigPath, "-n", namespace, "exec", "-i", podName, "-c", containerName, "--"}
	args := append(baseArgs, command...)
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = file
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl exec (stream in) 失敗: %v, stderr: %s", err, stderr.String())
	}
	return nil
}
func executeKubectlCommand(namespace string, args ...string) ([]byte, []byte, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get home dir: %w", err)
	}
	kubeconfigPath := filepath.Join(home, ".kube", "config")
	baseArgs := []string{"--kubeconfig", kubeconfigPath}
	if namespace != "" {
		baseArgs = append(baseArgs, "-n", namespace)
	}
	finalArgs := append(baseArgs, args...)
	cmd := exec.Command("kubectl", finalArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), stderr.Bytes(), fmt.Errorf("kubectl %v error: %v, stderr: %s", finalArgs, err, stderr.String())
	}
	return stdout.Bytes(), stderr.Bytes(), nil
}
func applyKubernetesYAML(namespace string, yamlContent string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home dir: %w", err)
	}
	kubeconfigPath := filepath.Join(home, ".kube", "config")
	cmd := exec.Command("kubectl", "--kubeconfig", kubeconfigPath, "-n", namespace, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yamlContent)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("kubectl apply (ns: %s) error: %v, stderr: %s", namespace, err, stderr.String())
	}
	return nil
}
func ensureNamespace(namespace string) error {
	log.Printf("Ensuring namespace '%s' exists...", namespace)
	output, stderr, err := executeKubectlCommand("", "get", "namespace", namespace)
	if err == nil {
		log.Printf("Namespace '%s' already exists.", namespace)
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "not found") || strings.Contains(strings.ToLower(string(output)), "not found") || strings.Contains(strings.ToLower(string(stderr)), "not found") {
		log.Printf("Namespace '%s' not found, creating...", namespace)
		_, _, createErr := executeKubectlCommand("", "create", "namespace", namespace)
		if createErr != nil {
			return fmt.Errorf("failed to create namespace '%s': %w", namespace, createErr)
		}
		log.Printf("Namespace '%s' created.", namespace)
		return nil
	}
	return fmt.Errorf("failed to check namespace '%s': %w", namespace, err)
}
func executeKubectlDelete(namespace string, resourceType, labelSelector string) error {
	output, stderr, err := executeKubectlCommand(namespace, "delete", resourceType, "-l", labelSelector)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") && !strings.Contains(strings.ToLower(string(output)), "not found") && !strings.Contains(strings.ToLower(string(stderr)), "not found") {
		return err
	}
	return nil
}
func getServicePorts(namespace string, labelSelector string) (map[string]int, error) {
	output, stderr, err := executeKubectlCommand(namespace, "get", "services", "-l", labelSelector, "-o", "json")
	if err != nil {
		if strings.Contains(strings.ToLower(string(output)), "no resources found") || strings.Contains(strings.ToLower(string(stderr)), "no resources found") {
			return nil, fmt.Errorf("no services found matching label %s", labelSelector)
		}
		return nil, err
	}
	if strings.Contains(strings.ToLower(string(output)), "no resources found") || strings.Contains(strings.ToLower(string(stderr)), "no resources found") {
		return nil, fmt.Errorf("no services found matching label %s", labelSelector)
	}
	ports := make(map[string]int)
	var serviceList struct {
		Items []struct {
			Spec struct {
				Ports []struct {
					Name     string `json:"name"`
					NodePort int    `json:"nodePort"`
				} `json:"ports"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(output, &serviceList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service list: %w", err)
	}
	for _, item := range serviceList.Items {
		for _, port := range item.Spec.Ports {
			if port.Name != "" {
				ports[port.Name] = port.NodePort
			}
		}
	}
	return ports, nil
}
func findPodNameByContainer(namespace string, labelSelector string, containerName string) (string, error) {
	var lastErr error
	for i := 0; i < 5; i++ {
		output, stderr, err := executeKubectlCommand(namespace, "get", "pods", "-l", labelSelector, "-o", "json")
		if err != nil {
			lastErr = fmt.Errorf("failed to get pods (stdout: %s, stderr: %s): %w", string(output), string(stderr), err)
			time.Sleep(2 * time.Second)
			continue
		}
		var podList struct {
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Spec struct {
					Containers []struct {
						Name string `json:"name"`
					} `json:"containers"`
				} `json:"spec"`
				Status struct {
					Phase string `json:"phase"`
				} `json:"status"`
			} `json:"items"`
		}
		if err := json.Unmarshal(output, &podList); err != nil {
			lastErr = fmt.Errorf("failed to unmarshal pod list: %w", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for _, item := range podList.Items {
			if item.Status.Phase != "Running" {
				continue
			}
			for _, container := range item.Spec.Containers {
				if container.Name == containerName {
					return item.Metadata.Name, nil
				}
			}
		}
		lastErr = fmt.Errorf("no running pod found in namespace %s with container %s", namespace, containerName)
		time.Sleep(2 * time.Second)
	}
	return "", lastErr
}
func sanitize(userID string) string {
	sanitized := strings.ToLower(userID)
	reg := regexp.MustCompile("[^a-z0-9-]")
	sanitized = reg.ReplaceAllString(sanitized, "-")
	if len(sanitized) > 40 {
		sanitized = sanitized[:40]
	}
	sanitized = strings.Trim(sanitized, "-")
	if len(sanitized) == 0 {
		return "invalid-user"
	}
	return sanitized
}
func waitForPod(namespace string, podName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Pod %s (ns: %s) did not become ready in time", podName, namespace)
		default:
			output, _, err := executeKubectlCommand(namespace, "get", "pod", podName, "-o", "jsonpath='{.status.phase}'")
			if err == nil && strings.Contains(string(output), "Running") {
				log.Printf("Pod '%s' (ns: %s) is Running.", podName, namespace)
				return nil
			}
			time.Sleep(2 * time.Second)
		}
	}
}

// --- フェーズ1：デフォルトポリシー定義 (変更なし) ---
const defaultResourceQuotaYAML = `
apiVersion: v1
kind: ResourceQuota
metadata:
  name: default-quota
spec:
  hard:
    requests.cpu: "2"
    requests.memory: "2Gi"
    limits.cpu: "4"
    limits.memory: "4Gi"
    requests.storage: "20Gi"
`
const defaultNetworkPolicyDenyAllYAML = `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
`
const defaultNetworkPolicyAllowlistYAML = `
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-platform-traffic
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kube-system
    ports:
    - protocol: UDP
      port: 53
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kube-system
      podSelector:
        matchLabels:
          app: keycloak
    ports:
    - protocol: TCP
      port: 8080
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: default
      podSelector:
        matchLabels:
          app: public-minio
    ports:
    - protocol: TCP
      port: 9000
  - to:
    - namespaceSelector: {}
      podSelector: {}
    ports:
    - protocol: TCP
      port: 5433
  - to:
    - namespaceSelector: {}
      podSelector: {}
    ports:
    - protocol: TCP
      port: 80
    - protocol: TCP
      port: 443
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: default
      podSelector:
        matchLabels:
          app: future-lab-backend
`

// --- Middlewares ---
type contextKey string

const userInfoKey contextKey = "userInfo"

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("panic recovered: %v", err)
				sendJSONError(w, "Internal Server Error (panic)", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			sendJSONError(w, "Authorizationヘッダーが必要です", http.StatusUnauthorized)
			return
		}
		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		if tokenString == authHeader {
			sendJSONError(w, "Bearerトークン形式ではありません", http.StatusUnauthorized)
			return
		}
		idToken, err := verifier.Verify(r.Context(), tokenString)
		if err != nil {
			log.Printf("トークンの検証に失敗しました: %v", err)
			sendJSONError(w, fmt.Sprintf("トークンが無効です: %s", err.Error()), http.StatusUnauthorized)
			return
		}
		ctx := context.WithValue(r.Context(), userInfoKey, idToken)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func adminMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userInfo, ok := r.Context().Value(userInfoKey).(*oidc.IDToken)
		if !ok {
			sendJSONError(w, "ユーザー情報がコンテキストにありません", http.StatusInternalServerError)
			return
		}
		var claims struct {
			RealmAccess struct {
				Roles []string `json:"roles"`
			} `json:"realm_access"`
		}
		if err := userInfo.Claims(&claims); err != nil {
			sendJSONError(w, "トークンクレームの解析に失敗しました", http.StatusInternalServerError)
			return
		}
		isAdmin := false
		for _, role := range claims.RealmAccess.Roles {
			if role == "admin" {
				isAdmin = true
				break
			}
		}
		if !isAdmin {
			sendJSONError(w, "アクセス拒否: 管理者権限が必要です", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- HTTP Handlers ---
func startExperimentHandler(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	userNamespace := "lab-user-" + sanitizedUserID

	var claims struct {
		Username string `json:"preferred_username"`
	}
	if err := userInfo.Claims(&claims); err != nil {
		sendJSONError(w, "Failed to parse user claims", http.StatusInternalServerError)
		return
	}
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.PackageName == "" {
		sendJSONError(w, "packageName is required", http.StatusBadRequest)
		return
	}
	log.Printf("Request from user '%s' (ID: %s) for package '%s' in namespace '%s'", claims.Username, sanitizedUserID, req.PackageName, userNamespace)

	if err := ensureNamespace(userNamespace); err != nil {
		sendJSONError(w, fmt.Sprintf("Namespaceの準備に失敗: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	log.Printf("Applying ResourceQuota to ns '%s'", userNamespace)
	if err := applyKubernetesYAML(userNamespace, defaultResourceQuotaYAML); err != nil {
		log.Printf("Failed to apply ResourceQuota: %v", err)
	}
	log.Printf("Applying NetworkPolicy (DenyAll) to ns '%s'", userNamespace)
	if err := applyKubernetesYAML(userNamespace, defaultNetworkPolicyDenyAllYAML); err != nil {
		log.Printf("Failed to apply DenyAll NetworkPolicy: %v", err)
	}
	log.Printf("Applying NetworkPolicy (Allowlist) to ns '%s'", userNamespace)
	if err := applyKubernetesYAML(userNamespace, defaultNetworkPolicyAllowlistYAML); err != nil {
		log.Printf("Failed to apply Allowlist NetworkPolicy: %v", err)
	}

	packageDir := filepath.Join("packages", req.PackageName)
	files, err := os.ReadDir(packageDir)
	if err != nil {
		sendJSONError(w, fmt.Sprintf("Package '%s' not found.", req.PackageName), http.StatusBadRequest)
		return
	}
	var yamlFiles []string
	for _, file := range files {
		if !file.IsDir() && (strings.HasSuffix(file.Name(), ".yaml") || strings.HasSuffix(file.Name(), ".yml")) {
			yamlFiles = append(yamlFiles, file.Name())
		}
	}
	sort.Strings(yamlFiles)

	for _, filename := range yamlFiles {
		log.Printf("Applying: %s for user %s in ns %s", filename, sanitizedUserID, userNamespace)
		yamlPath := filepath.Join(packageDir, filename)
		yamlBytes, err := os.ReadFile(yamlPath)
		if err != nil {
			sendJSONError(w, "Internal server error reading YAML file", http.StatusInternalServerError)
			return
		}

		finalYAML := strings.ReplaceAll(string(yamlBytes), "USER_ID_PLACEHOLDER", sanitizedUserID)

		lines := strings.Split(finalYAML, "\n")
		var modifiedLines []string
		for _, line := range lines {
			modifiedLines = append(modifiedLines, line)
			if strings.Contains(line, "user-id: ") {
				indent := strings.Repeat(" ", len(line)-len(strings.TrimLeft(line, " ")))
				modifiedLines = append(modifiedLines, fmt.Sprintf("%spackageName: %s", indent, req.PackageName))
			}
		}
		finalYAMLwithPackage := strings.Join(modifiedLines, "\n")

		if err := applyKubernetesYAML(userNamespace, finalYAMLwithPackage); err != nil {
			log.Printf("Failed to apply %s: %v", filename, err)
			sendJSONError(w, fmt.Sprintf("Kubernetes設定の適用に失敗: %s", err.Error()), http.StatusInternalServerError)
			return
		}
	}

	expected := ExpectedResources{
		UserID:      sanitizedUserID,
		Namespace:   userNamespace,
		PackageName: req.PackageName,
	}
	internalLedger.Store(userNamespace, expected)
	log.Printf("[Reconciler] Stored expected state for ns '%s'", userNamespace)

	log.Println("Waiting for services to be ready...")
	time.Sleep(5 * time.Second)
	servicePorts, err := getServicePorts(userNamespace, "user-id="+sanitizedUserID)
	if err != nil {
		log.Printf("ユーザーサービスポートの取得に失敗: %v", err)
		servicePorts = make(map[string]int)
	}

	publicServicePorts, err := getServicePorts("default", "app=public-minio")
	if err != nil {
		log.Printf("公開サービスポートの取得に失敗: %v", err)
		publicServicePorts = make(map[string]int)
	}
	sendJSONResponse(w, ExperimentResponse{
		Status:             "success",
		Message:            "Environment is ready.",
		ServicePorts:       servicePorts,
		PublicServicePorts: publicServicePorts,
		HostIP:             externalIP, // [新規]
	}, http.StatusOK)
}
func stopExperimentHandler(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	userNamespace := "lab-user-" + sanitizedUserID
	log.Printf("Stopping compute resources for user ID '%s' in namespace '%s'", sanitizedUserID, userNamespace)
	labelSelector := "user-id=" + sanitizedUserID
	resourcesToStop := []string{"deployments", "services"}
	for _, resource := range resourcesToStop {
		if err := executeKubectlDelete(userNamespace, resource, labelSelector); err != nil {
			sendJSONError(w, fmt.Sprintf("Failed to stop %s", resource), http.StatusInternalServerError)
			return
		}
	}
	sendJSONResponse(w, ExperimentResponse{Status: "success", Message: "Compute resources stopped. Data (PVC) is retained."}, http.StatusOK)
}
func deleteExperimentHandler(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	userNamespace := "lab-user-" + sanitizedUserID
	log.Printf("Deleting ALL resources for user ID '%s' by deleting namespace '%s'", sanitizedUserID, userNamespace)

	// 1. K8s Namespaceの削除 (従来通り)
	_, _, err := executeKubectlCommand("", "delete", "namespace", userNamespace)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
		sendJSONError(w, fmt.Sprintf("Failed to delete namespace %s", userNamespace), http.StatusInternalServerError)
		return
	}

	// 2. 内部台帳から削除 (従来通り)
	internalLedger.Delete(userNamespace)
	log.Printf("[Reconciler] Deleted expected state for ns '%s'", userNamespace)

	// 3. スナップショットのクリーンアップ (非公開のみ)
	log.Printf("[DB/MinIO] Cleaning up private snapshots for user '%s'", sanitizedUserID)
	ctx := context.Background()

	// 3a. まず、このユーザーが所有する全スナップショットをDBから検索
	query := `
		SELECT safe_name, visibility
		FROM snapshots
		WHERE owner_user_id = $1
	`
	rows, err := dbPool.Query(ctx, query, sanitizedUserID)
	if err != nil {
		log.Printf("[DB] WARNING: Failed to query snapshots for cleanup: %v", err)
		// エラーが発生しても、Namespace削除は成功しているので、続行
		sendJSONResponse(w, ExperimentResponse{Status: "success", Message: "All resources (Namespace) deleted (snapshot cleanup failed to query)."}, http.StatusOK)
		return
	}
	defer rows.Close()

	var privateSnapshotsToDeleteFromDB []string
	var privateObjectsToDeleteFromMinIO []minio.ObjectInfo

	// 3b. 検索結果をループし、「非公開」のものだけを削除リストに追加
	for rows.Next() {
		var safeName, visibility string
		if err := rows.Scan(&safeName, &visibility); err != nil {
			log.Printf("[DB] WARNING: Failed to scan snapshot row during cleanup: %v", err)
			continue
		}

		if visibility == "private" {
			log.Printf("[Delete] Marking private snapshot '%s' for deletion.", safeName)
			privateSnapshotsToDeleteFromDB = append(privateSnapshotsToDeleteFromDB, safeName)

			// 5つの実ファイルすべてをMinIO削除リストに追加
			objectKeys := []string{
				fmt.Sprintf("%s/%s/db.dump", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/minio.tar", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/workspace.tar", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/influxdb.tar", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/grafana.tar", sanitizedUserID, safeName),
			}
			for _, key := range objectKeys {
				privateObjectsToDeleteFromMinIO = append(privateObjectsToDeleteFromMinIO, minio.ObjectInfo{Key: key})
			}
		} else {
			log.Printf("[Delete] Keeping public snapshot '%s' (owner: %s).", safeName, sanitizedUserID)
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("[DB] WARNING: Error iterating snapshot rows: %v", err)
	}

	// 3c. MinIOから実ファイルを削除 (非公開分)
	if len(privateObjectsToDeleteFromMinIO) > 0 {
		log.Printf("[MinIO] Deleting %d objects for private snapshots...", len(privateObjectsToDeleteFromMinIO))
		objectsCh := make(chan minio.ObjectInfo)
		go func() {
			defer close(objectsCh)
			for _, obj := range privateObjectsToDeleteFromMinIO {
				objectsCh <- obj
			}
		}()

		for rErr := range minioClient.RemoveObjects(ctx, snapshotBucketName, objectsCh, minio.RemoveObjectsOptions{}) {
			if rErr.Err != nil {
				log.Printf("[MinIO] WARNING: Failed to delete object %s: %v", rErr.ObjectName, rErr.Err)
			}
		}
	}

	// 3d. DBからメタデータを削除 (非公開分)
	if len(privateSnapshotsToDeleteFromDB) > 0 {
		log.Printf("[DB] Deleting %d private snapshot metadata records...", len(privateSnapshotsToDeleteFromDB))
		deleteQuery := "DELETE FROM snapshots WHERE owner_user_id = $1 AND safe_name = ANY($2)"
		if _, err := dbPool.Exec(ctx, deleteQuery, sanitizedUserID, privateSnapshotsToDeleteFromDB); err != nil {
			log.Printf("[DB] WARNING: Failed to delete private snapshot metadata: %v", err)
		}
	}

	log.Printf("[Delete] Environment deletion and private snapshot cleanup complete for user '%s'", sanitizedUserID)
	sendJSONResponse(w, ExperimentResponse{Status: "success", Message: "All resources (Namespace) deleted. Public snapshots retained."}, http.StatusOK)
}
func executeSQLHandler(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	userNamespace := "lab-user-" + sanitizedUserID
	var req SQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	servicePorts, err := getServicePorts(userNamespace, "user-id="+sanitizedUserID)
	if err != nil || servicePorts["postgres"] == 0 {
		sendJSONError(w, fmt.Sprintf("Postgres port not found for user in namespace %s", userNamespace), http.StatusNotFound)
		return
	}
	connStr := fmt.Sprintf("postgres://user:mysecretpassword@localhost:%d/testdb", servicePorts["postgres"])
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		sendJSONError(w, "データベース接続に失敗しました。", http.StatusInternalServerError)
		return
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), req.SQLQuery)
	if err != nil {
		sendJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	headers := []string{}
	for _, fd := range rows.FieldDescriptions() {
		headers = append(headers, string(fd.Name))
	}
	results := [][]interface{}{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			sendJSONError(w, fmt.Sprintf("行データの読み取りに失敗しました: %v", err), http.StatusInternalServerError)
			return
		}
		results = append(results, values)
	}
	if err := rows.Err(); err != nil {
		sendJSONError(w, fmt.Sprintf("行のイテレーション中にエラーが発生しました: %v", err), http.StatusInternalServerError)
		return
	}
	sendJSONResponse(w, SQLResponse{Status: "success", Headers: headers, Rows: results, Message: "Query executed."}, http.StatusOK)
}
func handleAnalyze(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	userNamespace := "lab-user-" + sanitize(userInfo.Subject)
	r.ParseMultipartForm(10 << 20)
	file, _, err := r.FormFile("csvfile")
	if err != nil {
		sendJSONError(w, "ファイルアップロードの処理に失敗しました: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	tempFile, err := os.CreateTemp("", "upload-*.csv")
	if err != nil {
		sendJSONError(w, "一時ファイルの作成に失敗しました: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.Remove(tempFile.Name())
	io.Copy(tempFile, file)
	jobID := filepath.Base(tempFile.Name())
	podName := "analyzer-" + sanitize(jobID)
	podYAML := fmt.Sprintf(`
apiVersion: v1
kind: Pod
metadata:
  name: %s
spec:
  containers:
  - name: analysis-worker
    image: baumu373/analysis-worker:0.3
    command: ["sleep", "3600"]
    resources:          # <--- これを追加
      requests:
        cpu: "200m"
        memory: "256Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"
  restartPolicy: Never
`, podName)
	if err := applyKubernetesYAML(userNamespace, podYAML); err != nil {
		sendJSONError(w, "分析用Podの作成に失敗しました: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer executeKubectlCommand(userNamespace, "delete", "pod", podName, "--ignore-not-found=true")
	log.Printf("分析用Pod '%s' をNamespace '%s' に作成しました。", podName, userNamespace)
	if err := waitForPod(userNamespace, podName, 2*time.Minute); err != nil {
		sendJSONError(w, "分析用Podの起動タイムアウト: "+err.Error(), http.StatusInternalServerError)
		return
	}
	podFilePath := fmt.Sprintf("%s/%s:/app/input.csv", userNamespace, podName)
	if _, _, err := executeKubectlCommand("", "cp", tempFile.Name(), podFilePath); err != nil {
		if _, _, errRetry := executeKubectlCommand(userNamespace, "cp", tempFile.Name(), fmt.Sprintf("%s:/app/input.csv", podName)); errRetry != nil {
			sendJSONError(w, "Podへのファイルコピーに失敗しました: "+errRetry.Error(), http.StatusInternalServerError)
			return
		}
	}
	log.Printf("Pod '%s' (ns: %s) 内で分析を実行します...", podName, userNamespace)
	_, _, err = executeKubectlCommand(userNamespace, "exec", podName, "--", "python", "main.py", "input.csv", "output.png")
	if err != nil {
		sendJSONError(w, "Pod内での分析実行に失敗しました: "+err.Error(), http.StatusInternalServerError)
		return
	}
	localImagePath := filepath.Join(os.TempDir(), "output-"+jobID+".png")
	defer os.Remove(localImagePath)
	podImagePath := fmt.Sprintf("%s/%s:/app/output.png", userNamespace, podName)
	if _, _, err := executeKubectlCommand("", "cp", podImagePath, localImagePath); err != nil {
		if _, _, errRetry := executeKubectlCommand(userNamespace, "cp", fmt.Sprintf("%s:/app/output.png", podName), localImagePath); errRetry != nil {
			sendJSONError(w, "Podからの結果ファイルコピーに失敗しました: "+errRetry.Error(), http.StatusInternalServerError)
			return
		}
	}
	log.Printf("分析完了。結果をクライアントに送信します。")
	http.ServeFile(w, r, localImagePath)
}

// --- Admin Handlers (変更なし) ---
func handleListExperiments(w http.ResponseWriter, r *http.Request) {
	output, _, err := executeKubectlCommand("", "get", "deployments", "-A", "-l", "user-id", "-o", "json")
	if err != nil {
		sendJSONError(w, "稼働中の環境リストの取得に失敗しました: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var deploymentList struct {
		Items []struct {
			Metadata struct {
				Name              string            `json:"name"`
				Namespace         string            `json:"namespace"`
				CreationTimestamp string            `json:"creationTimestamp"`
				Labels            map[string]string `json:"labels"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(output, &deploymentList); err != nil {
		sendJSONError(w, "Kubernetesからの応答の解析に失敗しました: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var experiments []ActiveExperiment
	for _, item := range deploymentList.Items {
		experiments = append(experiments, ActiveExperiment{
			UserID:            item.Metadata.Labels["user-id"],
			DeploymentName:    item.Metadata.Name,
			PackageName:       item.Metadata.Labels["packageName"],
			CreationTimestamp: item.Metadata.CreationTimestamp,
			Namespace:         item.Metadata.Namespace,
		})
	}
	sendJSONResponse(w, experiments, http.StatusOK)
}
func handleAdminStopExperiment(w http.ResponseWriter, r *http.Request) {
	var req AdminActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.UserID == "" {
		sendJSONError(w, "userId is required", http.StatusBadRequest)
		return
	}
	sanitizedUserID := sanitize(req.UserID)
	userNamespace := "lab-user-" + sanitizedUserID
	log.Printf("ADMIN ACTION: Stopping compute resources for user ID '%s' in namespace '%s'", sanitizedUserID, userNamespace)
	labelSelector := "user-id=" + sanitizedUserID
	resourcesToStop := []string{"deployments", "services"}
	for _, resource := range resourcesToStop {
		if err := executeKubectlDelete(userNamespace, resource, labelSelector); err != nil {
			sendJSONError(w, fmt.Sprintf("Failed to stop %s for user %s (ns: %s)", resource, sanitizedUserID, userNamespace), http.StatusInternalServerError)
			return
		}
	}
	sendJSONResponse(w, map[string]string{"status": "success", "message": "Compute resources stopped for user " + sanitizedUserID}, http.StatusOK)
}
func handleAdminDeleteExperiment(w http.ResponseWriter, r *http.Request) {
	var req AdminActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.UserID == "" {
		sendJSONError(w, "userId is required", http.StatusBadRequest)
		return
	}
	sanitizedUserID := sanitize(req.UserID)
	userNamespace := "lab-user-" + sanitizedUserID
	log.Printf("ADMIN ACTION: Deleting ALL resources for user ID '%s' by deleting namespace '%s'", sanitizedUserID, userNamespace)

	_, _, err := executeKubectlCommand("", "delete", "namespace", userNamespace)
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
		sendJSONError(w, fmt.Sprintf("Failed to delete namespace %s for user %s", userNamespace, sanitizedUserID), http.StatusInternalServerError)
		return
	}

	internalLedger.Delete(userNamespace)
	log.Printf("[Reconciler] Deleted expected state for ns '%s' (Admin Action)", userNamespace)

	log.Printf("[DB/MinIO] Cleaning up private snapshots for user '%s' (Admin Action)", sanitizedUserID)
	ctx := context.Background()
	query := `SELECT safe_name, visibility FROM snapshots WHERE owner_user_id = $1`
	rows, err := dbPool.Query(ctx, query, sanitizedUserID)
	if err != nil {
		log.Printf("[DB] WARNING: Failed to query snapshots for cleanup (Admin Action): %v", err)
		sendJSONResponse(w, map[string]string{"status": "success", "message": "All resources (Namespace) deleted (snapshot cleanup failed to query) for user " + sanitizedUserID}, http.StatusOK)
		return
	}
	defer rows.Close()

	var privateSnapshotsToDeleteFromDB []string
	var privateObjectsToDeleteFromMinIO []minio.ObjectInfo

	for rows.Next() {
		var safeName, visibility string
		if err := rows.Scan(&safeName, &visibility); err != nil {
			log.Printf("[DB] WARNING: Failed to scan snapshot row during cleanup (Admin Action): %v", err)
			continue
		}
		if visibility == "private" {
			privateSnapshotsToDeleteFromDB = append(privateSnapshotsToDeleteFromDB, safeName)
			objectKeys := []string{
				fmt.Sprintf("%s/%s/db.dump", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/minio.tar", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/workspace.tar", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/influxdb.tar", sanitizedUserID, safeName),
				fmt.Sprintf("%s/%s/grafana.tar", sanitizedUserID, safeName),
			}
			for _, key := range objectKeys {
				privateObjectsToDeleteFromMinIO = append(privateObjectsToDeleteFromMinIO, minio.ObjectInfo{Key: key})
			}
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("[DB] WARNING: Error iterating snapshot rows (Admin Action): %v", err)
	}
	if len(privateObjectsToDeleteFromMinIO) > 0 {
		objectsCh := make(chan minio.ObjectInfo)
		go func() {
			defer close(objectsCh)
			for _, obj := range privateObjectsToDeleteFromMinIO {
				objectsCh <- obj
			}
		}()
		for rErr := range minioClient.RemoveObjects(ctx, snapshotBucketName, objectsCh, minio.RemoveObjectsOptions{}) {
			if rErr.Err != nil {
				log.Printf("[MinIO] WARNING: Failed to delete object %s (Admin Action): %v", rErr.ObjectName, rErr.Err)
			}
		}
	}
	if len(privateSnapshotsToDeleteFromDB) > 0 {
		deleteQuery := "DELETE FROM snapshots WHERE owner_user_id = $1 AND safe_name = ANY($2)"
		if _, err := dbPool.Exec(ctx, deleteQuery, sanitizedUserID, privateSnapshotsToDeleteFromDB); err != nil {
			log.Printf("[DB] WARNING: Failed to delete private snapshot metadata (Admin Action): %v", err)
		}
	}
	log.Printf("[Admin Action] Environment deletion and private snapshot cleanup complete for user '%s'", sanitizedUserID)
	sendJSONResponse(w, map[string]string{"status": "success", "message": "All resources (Namespace) deleted and private snapshots cleaned up for user " + sanitizedUserID}, http.StatusOK)
}

// [修正] handleAdminListSnapshots (COALESCE)
func handleAdminListSnapshots(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	log.Printf("[Admin DB] Listing ALL snapshots")

	// [修正] COALESCE を使い、NULLの可能性がある owner_username を '' (空文字列) に変換
	query := `
		SELECT safe_name, display_name, owner_user_id, COALESCE(owner_username, ''), visibility, created_at
		FROM snapshots
		ORDER BY created_at DESC
	`
	rows, err := dbPool.Query(ctx, query)
	if err != nil {
		sendJSONError(w, "スナップショット一覧のDB問合せに失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var snapshots []SnapshotInfo
	for rows.Next() {
		var info SnapshotInfo
		var visibility string
		// [修正] Scan先の info.OwnerUsername は *string ではなく string なので、 NULL -> '' への変換が必須
		err := rows.Scan(&info.Name, &info.DisplayName, &info.OwnerUserID, &info.OwnerUsername, &visibility, &info.LastModified)
		if err != nil {
			log.Printf("[Admin DB] Failed to scan snapshot row: %v", err)
			continue
		}
		info.IsPublic = (visibility == "public")
		snapshots = append(snapshots, info)
	}

	if err := rows.Err(); err != nil {
		sendJSONError(w, "スナップショット一覧のDB読取に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}

	sendJSONResponse(w, snapshots, http.StatusOK)
}
func handleAdminDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	var req AdminDeleteSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.SafeName == "" || req.OwnerUserID == "" {
		sendJSONError(w, "safeName and ownerUserID are required", http.StatusBadRequest)
		return
	}

	log.Printf("[Admin Action] Deleting snapshot (safeName: %s, owner: %s)", req.SafeName, req.OwnerUserID)
	ctx := context.Background()

	objectNames := []string{
		fmt.Sprintf("%s/%s/db.dump", req.OwnerUserID, req.SafeName),
		fmt.Sprintf("%s/%s/minio.tar", req.OwnerUserID, req.SafeName),
		fmt.Sprintf("%s/%s/workspace.tar", req.OwnerUserID, req.SafeName),
		fmt.Sprintf("%s/%s/influxdb.tar", req.OwnerUserID, req.SafeName),
		fmt.Sprintf("%s/%s/grafana.tar", req.OwnerUserID, req.SafeName),
	}
	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for _, name := range objectNames {
			objectsCh <- minio.ObjectInfo{Key: name}
		}
	}()

	log.Printf("[Admin MinIO] Removing %d objects...", len(objectNames))
	for rErr := range minioClient.RemoveObjects(ctx, snapshotBucketName, objectsCh, minio.RemoveObjectsOptions{}) {
		if rErr.Err != nil {
			log.Printf("[Admin MinIO] WARNING: Failed to delete object %s: %v", rErr.ObjectName, rErr.Err)
		}
	}
	log.Printf("[Admin MinIO] Object deletion complete.")

	log.Printf("[Admin DB] Deleting snapshot metadata from DB...")
	deleteQuery := "DELETE FROM snapshots WHERE safe_name = $1 AND owner_user_id = $2"
	tag, err := dbPool.Exec(ctx, deleteQuery, req.SafeName, req.OwnerUserID)
	if err != nil {
		sendJSONError(w, "DBからのメタデータ削除に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		sendJSONError(w, "DBから対象のメタデータが見つかりませんでした (MinIOファイルは削除済み)", http.StatusNotFound)
		return
	}

	log.Printf("[Admin Action] Snapshot (safeName: %s) deleted successfully.", req.SafeName)
	sendJSONResponse(w, map[string]string{"status": "success", "message": "Snapshot " + req.SafeName + " (DB metadata and MinIO files) deleted."}, http.StatusOK)
}

// --- スナップショットハンドラ (User) (変更なし) ---
type snapshotTarget struct {
	containerName   string
	localFile       string
	minioObject     string
	backupCmd       []string
	restoreCmd      []string
	restoreCleanCmd []string
}

func handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	userNamespace := "lab-user-" + sanitizedUserID
	labelSelector := "user-id=" + sanitizedUserID

	var claims struct {
		Username string `json:"preferred_username"`
	}
	if err := userInfo.Claims(&claims); err != nil {
		log.Printf("[Snapshot] WARNING: Could not parse username from token: %v", err)
		claims.Username = ""
	}
	ownerUsername := claims.Username

	var req SnapshotCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	displayName := req.SnapshotName
	if displayName == "" {
		sendJSONError(w, "スナップショット名の指定は必須です", http.StatusBadRequest)
		return
	}
	safeSnapshotName := sanitize(displayName)
	if safeSnapshotName == "" {
		safeSnapshotName = "snapshot"
	}
	safeSnapshotName = fmt.Sprintf("%s-%d", safeSnapshotName, time.Now().Unix())

	visibility := "private"
	if req.IsPublic {
		visibility = "public"
	}

	log.Printf("[Snapshot] Creating snapshot '%s' (safe name: %s) for ns '%s'", displayName, safeSnapshotName, userNamespace)

	localTempDir, err := os.MkdirTemp("", "snapshot-"+sanitizedUserID)
	if err != nil {
		sendJSONError(w, "一時ディレクトリの作成に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(localTempDir)

	podName, err := findPodNameByContainer(userNamespace, labelSelector, "postgres-container")
	if err != nil {
		sendJSONError(w, "実行中のPodが見つかりません: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[Snapshot] Found target pod '%s' in ns '%s'", podName, userNamespace)
	log.Printf("[Snapshot] [Compromise] 実行中のPodに対してスナップショットを実行します (整合性保証なし)")

	targets := []snapshotTarget{
		{
			containerName: "postgres-container",
			localFile:     filepath.Join(localTempDir, "db.dump"),
			minioObject:   fmt.Sprintf("%s/%s/db.dump", sanitizedUserID, safeSnapshotName),
			backupCmd:     []string{"pg_dump", "-U", "user", "-d", "testdb", "-F", "c"},
		},
		{
			containerName: "minio-container",
			localFile:     filepath.Join(localTempDir, "minio.tar"),
			minioObject:   fmt.Sprintf("%s/%s/minio.tar", sanitizedUserID, safeSnapshotName),
			backupCmd:     []string{"/minio-tools/tar", "cf", "-", "/data"},
		},
		{
			containerName: "analysis-container",
			localFile:     filepath.Join(localTempDir, "workspace.tar"),
			minioObject:   fmt.Sprintf("%s/%s/workspace.tar", sanitizedUserID, safeSnapshotName),
			backupCmd:     []string{"tar", "cf", "-", "-C", "/workspace", "."},
		},
		{
			containerName: "influxdb-container",
			localFile:     filepath.Join(localTempDir, "influxdb.tar"),
			minioObject:   fmt.Sprintf("%s/%s/influxdb.tar", sanitizedUserID, safeSnapshotName),
			backupCmd:     []string{"tar", "cf", "-", "-C", "/var/lib/influxdb2", "."},
		},
		{
			containerName: "grafana-container",
			localFile:     filepath.Join(localTempDir, "grafana.tar"),
			minioObject:   fmt.Sprintf("%s/%s/grafana.tar", sanitizedUserID, safeSnapshotName),
			backupCmd:     []string{"tar", "cf", "-", "-C", "/var/lib/grafana", "."},
		},
	}
	for _, target := range targets {
		log.Printf("[Snapshot] Backing up %s...", target.containerName)
		err := executeKubectlExecToFile(userNamespace, podName, target.containerName, target.localFile, target.backupCmd...)
		if err != nil {
			sendJSONError(w, fmt.Sprintf("%s のバックアップに失敗: %v", target.containerName, err), http.StatusInternalServerError)
			return
		}
	}
	log.Printf("[Snapshot] Uploading to MinIO bucket '%s'", snapshotBucketName)
	for _, target := range targets {
		if _, err := minioClient.FPutObject(context.Background(), snapshotBucketName, target.minioObject, target.localFile, minio.PutObjectOptions{}); err != nil {
			sendJSONError(w, fmt.Sprintf("%s のアップロードに失敗: %v", target.localFile, err), http.StatusInternalServerError)
			return
		}
	}
	log.Printf("[DB] Inserting snapshot metadata into DB...")
	ctx := context.Background()
	insertQuery := `
		INSERT INTO snapshots (id, owner_user_id, owner_username, safe_name, display_name, visibility, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err = dbPool.Exec(ctx, insertQuery,
		uuid.New().String(), // id
		sanitizedUserID,     // owner_user_id
		ownerUsername,       // owner_username
		safeSnapshotName,    // safe_name
		displayName,         // display_name
		visibility,          // visibility
		time.Now(),          // created_at
	)
	if err != nil {
		sendJSONError(w, "スナップショットのメタデータ保存に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[Snapshot] Snapshot '%s' created successfully for ns '%s'", displayName, userNamespace)
	sendJSONResponse(w, map[string]string{"status": "success", "message": "Snapshot '" + displayName + "' created successfully."}, http.StatusOK)
}

// [修正] handleListSnapshots (COALESCE)
func handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	ctx := context.Background()
	log.Printf("[DB] Listing snapshots for user '%s'", sanitizedUserID)

	// [修正] COALESCE を使い、NULLの可能性がある owner_username を '' (空文字列) に変換
	query := `
		SELECT safe_name, display_name, owner_user_id, COALESCE(owner_username, ''), visibility, created_at
		FROM snapshots
		WHERE owner_user_id = $1 OR visibility = 'public'
		ORDER BY created_at DESC
	`
	rows, err := dbPool.Query(ctx, query, sanitizedUserID)
	if err != nil {
		sendJSONError(w, "スナップショット一覧のDB問合せに失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var snapshots []SnapshotInfo
	for rows.Next() {
		var info SnapshotInfo
		var visibility string
		// [修正] Scan先の info.OwnerUsername は *string ではなく string なので、 NULL -> '' への変換が必須
		err := rows.Scan(&info.Name, &info.DisplayName, &info.OwnerUserID, &info.OwnerUsername, &visibility, &info.LastModified)
		if err != nil {
			log.Printf("[DB] Failed to scan snapshot row: %v", err)
			continue
		}
		info.IsPublic = (visibility == "public")
		snapshots = append(snapshots, info)
	}

	if err := rows.Err(); err != nil {
		sendJSONError(w, "スナップショット一覧のDB読取に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	sendJSONResponse(w, snapshots, http.StatusOK)
}
func handleRestoreSnapshot(w http.ResponseWriter, r *http.Request) {
	userInfo := r.Context().Value(userInfoKey).(*oidc.IDToken)
	sanitizedUserID := sanitize(userInfo.Subject)
	userNamespace := "lab-user-" + sanitizedUserID
	labelSelector := "user-id=" + sanitizedUserID
	var req SnapshotRestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSONError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	safeSnapshotName := req.SnapshotName
	if safeSnapshotName == "" {
		sendJSONError(w, "snapshotName is required", http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	log.Printf("[Restore] Restoring snapshot '%s' for ns '%s'", safeSnapshotName, userNamespace)
	var ownerUserID string
	query := `
		SELECT owner_user_id
		FROM snapshots
		WHERE safe_name = $1 AND (owner_user_id = $2 OR visibility = 'public')
	`
	err := dbPool.QueryRow(ctx, query, safeSnapshotName, sanitizedUserID).Scan(&ownerUserID)
	if err != nil {
		if err == pgx.ErrNoRows {
			sendJSONError(w, "スナップショットが見つからないか、アクセス権がありません", http.StatusNotFound)
			return
		}
		sendJSONError(w, "スナップショットのDB検証に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("[Restore] Snapshot owner is '%s'. Restore target is '%s'.", ownerUserID, sanitizedUserID)
	podName, err := findPodNameByContainer(userNamespace, labelSelector, "postgres-container")
	if err != nil {
		sendJSONError(w, "復元先のPodが見つかりません。先に環境を「開始」してください: "+err.Error(), http.StatusNotFound)
		return
	}
	log.Printf("[Restore] Found target pod '%s' in ns '%s'", podName, userNamespace)
	localTempDir, err := os.MkdirTemp("", "restore-"+sanitizedUserID)
	if err != nil {
		sendJSONError(w, "一時ディレクトリの作成に失敗: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(localTempDir)
	targets := []snapshotTarget{
		{
			containerName: "postgres-container",
			localFile:     filepath.Join(localTempDir, "db.dump"),
			minioObject:   fmt.Sprintf("%s/%s/db.dump", ownerUserID, safeSnapshotName),
			restoreCmd:    []string{"pg_restore", "-U", "user", "-d", "testdb", "-F", "c", "-c", "/dev/stdin"},
		},
		{
			containerName: "minio-container",
			localFile:     filepath.Join(localTempDir, "minio.tar"),
			minioObject:   fmt.Sprintf("%s/%s/minio.tar", ownerUserID, safeSnapshotName),
			restoreCmd:    []string{"/minio-tools/tar", "xf", "-", "-C", "/data"},
			restoreCleanCmd: []string{"sh", "-c", "rm -rf /data/*"},
		},
		{
			containerName: "analysis-container",
			localFile:     filepath.Join(localTempDir, "workspace.tar"),
			minioObject:   fmt.Sprintf("%s/%s/workspace.tar", ownerUserID, safeSnapshotName),
			restoreCmd:    []string{"tar", "xf", "-", "-C", "/workspace"},
			restoreCleanCmd: []string{"sh", "-c", "rm -rf /workspace/*"},
		},
		{
			containerName: "influxdb-container",
			localFile:     filepath.Join(localTempDir, "influxdb.tar"),
			minioObject:   fmt.Sprintf("%s/%s/influxdb.tar", ownerUserID, safeSnapshotName),
			restoreCmd:    []string{"tar", "xf", "-", "-C", "/var/lib/influxdb2"},
			restoreCleanCmd: []string{"sh", "-c", "rm -rf /var/lib/influxdb2/*"},
		},
		{
			containerName: "grafana-container",
			localFile:     filepath.Join(localTempDir, "grafana.tar"),
			minioObject:   fmt.Sprintf("%s/%s/grafana.tar", ownerUserID, safeSnapshotName),
			restoreCmd:    []string{"tar", "xf", "-", "-C", "/var/lib/grafana"},
			restoreCleanCmd: []string{"sh", "-c", "rm -rf /var/lib/grafana/*"},
		},
	}
	log.Printf("[Restore] Downloading files from MinIO (owner: %s)...", ownerUserID)
	for _, target := range targets {
		if err := minioClient.FGetObject(context.Background(), snapshotBucketName, target.minioObject, target.localFile, minio.GetObjectOptions{}); err != nil {
			sendJSONError(w, fmt.Sprintf("%s のダウンロードに失敗: %v", target.minioObject, err), http.StatusInternalServerError)
			return
		}
	}
	log.Printf("[Restore] Executing restore commands in Pods (target: %s)...", sanitizedUserID)
	for _, target := range targets {
		if target.restoreCleanCmd != nil {
			log.Printf("[Restore] Cleaning %s...", target.containerName)
			if _, stderr, err := executeKubectlCommand(userNamespace, append([]string{"exec", podName, "-c", target.containerName, "--"}, target.restoreCleanCmd...)...); err != nil {
				log.Printf("[Restore] WARNING: %s のクリーンアップに失敗: %v, stderr: %s", target.containerName, err, string(stderr))
			}
		}
		log.Printf("[Restore] Restoring %s...", target.containerName)
		err := executeKubectlExecStreamIn(userNamespace, podName, target.containerName, target.localFile, target.restoreCmd...)
		if err != nil {
			sendJSONError(w, fmt.Sprintf("%s のリストアに失敗: %v", target.containerName, err), http.StatusInternalServerError)
			return
		}
	}
	log.Printf("[Restore] Restore of snapshot '%s' completed for ns '%s'", safeSnapshotName, userNamespace)
	sendJSONResponse(w, map[string]string{"status": "success", "message": "Snapshot '" + safeSnapshotName + "' restored successfully."}, http.StatusOK)
}

// --- CORS Middleware (変更) ---
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// [変更] 任意のオリジンからのアクセスを許可する（または特定のIP範囲に制限する）
		// スマホからアクセスする場合、Originヘッダーは "http://192.168.x.x:8000" などになる
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		} else {
			// Originヘッダーがない場合（curlなど）はワイルドカードでも良いが、Credentialを含む場合は不可
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		
		// Preflightリクエストへの対応
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- フェーズ2：自作コントローラー（調整ループ） (変更なし) ---
func reconcileUser(namespace string, expected ExpectedResources) {
	log.Printf("[Reconciler] Checking state for ns '%s'...", namespace)
	labelSelector := "user-id=" + expected.UserID
	outputDep, stderrDep, errDep := executeKubectlCommand(namespace, "get", "deployment", "-l", labelSelector)
	if errDep != nil || strings.Contains(strings.ToLower(string(outputDep)), "no resources found") || strings.Contains(strings.ToLower(string(stderrDep)), "no resources found") {
		if errDep != nil {
			log.Printf("[Reconciler] Error checking Deployment: %v", errDep)
		}
		log.Printf("[Reconciler] WARNING: Deployment for ns '%s' not found! (stdout: %s, stderr: %s) Re-applying package '%s'", namespace, string(outputDep), string(stderrDep), expected.PackageName)
		reApplyPackage(namespace, expected.PackageName, expected.UserID)
		return
	}
	outputSvc, stderrSvc, errSvc := executeKubectlCommand(namespace, "get", "service", "-l", labelSelector)
	if errSvc != nil || strings.Contains(strings.ToLower(string(outputSvc)), "no resources found") || strings.Contains(strings.ToLower(string(stderrSvc)), "no resources found") {
		if errSvc != nil {
			log.Printf("[Reconciler] Error checking Service: %v", errSvc)
		}
		log.Printf("[Reconciler] WARNING: Service for ns '%s' not found! (stdout: %s, stderr: %s) Re-applying package '%s'", namespace, string(outputSvc), string(stderrSvc), expected.PackageName)
		reApplyPackage(namespace, expected.PackageName, expected.UserID)
		return
	}
	log.Printf("[Reconciler] State for ns '%s' is OK.", namespace)
}
func reApplyPackage(namespace string, packageName string, sanitizedUserID string) {
	packageDir := filepath.Join("packages", packageName)
	files, err := os.ReadDir(packageDir)
	if err != nil {
		log.Printf("[Reconciler] Error re-applying package: cannot read package dir %s: %v", packageDir, err)
		return
	}
	var yamlFiles []string
	for _, file := range files {
		if !file.IsDir() && (strings.HasSuffix(file.Name(), ".yaml") || strings.HasSuffix(file.Name(), ".yml")) {
			yamlFiles = append(yamlFiles, file.Name())
		}
	}
	sort.Strings(yamlFiles)
	for _, filename := range yamlFiles {
		if strings.HasPrefix(filename, "1") {
			continue
		}
		log.Printf("[Reconciler] Re-applying: %s for user %s in ns %s", filename, sanitizedUserID, namespace)
		yamlPath := filepath.Join(packageDir, filename)
		yamlBytes, err := os.ReadFile(yamlPath)
		if err != nil {
			log.Printf("[Reconciler] Error re-applying %s: %v", filename, err)
			continue
		}
		finalYAML := strings.ReplaceAll(string(yamlBytes), "USER_ID_PLACEHOLDER", sanitizedUserID)
		lines := strings.Split(finalYAML, "\n")
		var modifiedLines []string
		for _, line := range lines {
			modifiedLines = append(modifiedLines, line)
			if strings.Contains(line, "user-id: ") {
				indent := strings.Repeat(" ", len(line)-len(strings.TrimLeft(line, " ")))
				modifiedLines = append(modifiedLines, fmt.Sprintf("%spackageName: %s", indent, packageName))
			}
		}
		finalYAMLwithPackage := strings.Join(modifiedLines, "\n")
		if err := applyKubernetesYAML(namespace, finalYAMLwithPackage); err != nil {
			log.Printf("[Reconciler] Failed to re-apply %s: %v", filename, err)
		}
	}
}
func runCustomReconciler() {
	log.Println("[Reconciler] Starting custom self-healing controller loop (check interval: 30s)...")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		log.Println("[Reconciler] Tick. Checking all active environments...")
		internalLedger.Range(func(key, value interface{}) bool {
			namespace, okNs := key.(string)
			expected, okExp := value.(ExpectedResources)
			if !okNs || !okExp {
				log.Printf("[Reconciler] ERROR: Invalid data in internal ledger. Key: %v", key)
				return true
			}
			reconcileUser(namespace, expected)
			return true
		})
	}
}

// --- DB初期化関数 (変更なし) ---
func initDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
	log.Printf("[DB] Connecting to metadata DB at %s:%d...", dbHost, dbPort)

	var pool *pgxpool.Pool
	var err error

	const maxRetries = 5
	for i := 0; i < maxRetries; i++ {
		pool, err = pgxpool.Connect(ctx, connStr)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				log.Println("[DB] Metadata DB connection established.")
				break
			}
		}
		if i < maxRetries-1 {
			log.Printf("[DB] Connection failed (attempt %d/%d), retrying in %d seconds... (error: %v)", i+1, maxRetries, (i+1)*2, err)
			time.Sleep(time.Duration(i+1) * 2 * time.Second)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("メタデータDBへの接続に失敗 (retries exhausted): %w", err)
	}

	createTableQuery := `
		CREATE TABLE IF NOT EXISTS snapshots (
			id UUID PRIMARY KEY,
			owner_user_id VARCHAR(255) NOT NULL,
			owner_username VARCHAR(255),
			safe_name VARCHAR(255) NOT NULL,
			display_name TEXT NOT NULL,
			visibility VARCHAR(50) NOT NULL DEFAULT 'private',
			created_at TIMESTPTZ NOT NULL,
			UNIQUE(owner_user_id, safe_name)
		);
	`
	_, err = pool.Exec(ctx, createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("snapshots テーブルの作成に失敗: %w", err)
	}

	alterQuery := `ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS owner_username VARCHAR(255)`
	if _, alterErr := pool.Exec(ctx, alterQuery); alterErr != nil {
		log.Printf("[DB] WARNING: ALTER TABLE snapshots (add owner_username) failed: %v", alterErr)
	}

	log.Printf("[DB] Metadata DB connected and 'snapshots' table ensured.")
	return pool, nil
}

// --- main関数 ---
func main() {
	var err error
	ctx := context.Background()

	// [変更] KeycloakのIssuer URLも環境変数ベースになるためログに出しておく
	log.Printf("Using Keycloak Issuer: %s", keycloakIssuer)

	provider, err = oidc.NewProvider(ctx, keycloakIssuer)
	if err != nil {
		log.Fatalf("OIDCプロバイダーの初期化に失敗: %v", err)
	}
    	// [修正] SkipIssuerCheck: true を追加
    	// コンテナ環境やIPアクセス時の「Issuer不一致エラー」を回避するため、
    	// URL文字列の厳密なチェックのみをスキップします（署名の検証は行われます）。
	verifier = provider.Verifier(&oidc.Config{
		ClientID:        keycloakClientID,
		SkipIssuerCheck: true, 
	})

	

	log.Printf("Initializing MinIO client for snapshots...")
	mc, err := minio.New(snapshotEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(snapshotAccessKeyID, snapshotSecretAccessKey, ""),
		Secure: snapshotUseSSL,
	})
	if err != nil {
		log.Fatalf("MinIOクライアントの初期化に失敗: %v", err)
	}
	minioClient = mc
	exists, err := minioClient.BucketExists(ctx, snapshotBucketName)
	if err != nil {
		log.Fatalf("MinIOバケットの確認に失敗: %v", err)
	}
	if !exists {
		log.Printf("Bucket '%s' not found, creating...", snapshotBucketName)
		err = minioClient.MakeBucket(ctx, snapshotBucketName, minio.MakeBucketOptions{})
		if err != nil {
			log.Fatalf("MinIOバケットの作成に失敗: %v", err)
		}
	} else {
		log.Printf("MinIO Bucket '%s' already exists.", snapshotBucketName)
	}

	dbPool, err = initDatabase(ctx)
	if err != nil {
		log.Fatalf("データベースの初期化に失敗: %v", err)
	}
	defer dbPool.Close()

	go runCustomReconciler()

	mux := http.NewServeMux()
	mux.Handle("/start-experiment", jwtMiddleware(http.HandlerFunc(startExperimentHandler)))
	mux.Handle("/stop-experiment", jwtMiddleware(http.HandlerFunc(stopExperimentHandler)))
	mux.Handle("/delete-experiment", jwtMiddleware(http.HandlerFunc(deleteExperimentHandler)))
	mux.Handle("/execute-sql", jwtMiddleware(http.HandlerFunc(executeSQLHandler)))
	mux.Handle("/analyze", jwtMiddleware(http.HandlerFunc(handleAnalyze)))
	mux.Handle("/snapshot/create", jwtMiddleware(http.HandlerFunc(handleCreateSnapshot)))
	mux.Handle("/snapshot/list", jwtMiddleware(http.HandlerFunc(handleListSnapshots)))
	mux.Handle("/snapshot/restore", jwtMiddleware(http.HandlerFunc(handleRestoreSnapshot)))
	mux.Handle("/admin/experiments", jwtMiddleware(adminMiddleware(http.HandlerFunc(handleListExperiments))))
	mux.Handle("/admin/stop-experiment", jwtMiddleware(adminMiddleware(http.HandlerFunc(handleAdminStopExperiment))))
	mux.Handle("/admin/delete-experiment", jwtMiddleware(adminMiddleware(http.HandlerFunc(handleAdminDeleteExperiment))))
	mux.Handle("/admin/snapshots/list", jwtMiddleware(adminMiddleware(http.HandlerFunc(handleAdminListSnapshots))))
	mux.Handle("/admin/snapshots/delete", jwtMiddleware(adminMiddleware(http.HandlerFunc(handleAdminDeleteSnapshot))))

	handler := corsMiddleware(mux)
	finalHandler := recoverMiddleware(handler)

	log.Println("Starting server on port 8090...")
	if err := http.ListenAndServe(":8090", finalHandler); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}