package main

//
//// TODO WIP clean this up
//
//
//
//import (
//	"context"
//	"errors"
//	"fmt"
//	"log"
//	"os"
//	s "strings"
//	"time"
//
//	"code.gitea.io/sdk/gitea"
//	appsv1 "k8s.io/api/apps/v1"
//	v1 "k8s.io/api/core/v1"
//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
//	"k8s.io/client-go/kubernetes"
//	"k8s.io/client-go/tools/clientcmd"
//)
//
//var secretsNamespace = "global-secrets"
//
//type Organization struct {
//	Name        string
//	Description string
//}
//
//type Repository struct {
//	Name    string
//	Owner   string
//	Private bool
//	Migrate struct {
//		Source string
//		Mirror bool
//	}
//}
//
//type Token struct {
//	Name string
//}
//type Config struct {
//	Organizations []Organization
//	Repositories  []Repository
//	Tokens        []Token
//}
//
//func getKubernetesClient() (*kubernetes.Clientset, error) {
//	rules := clientcmd.NewDefaultClientConfigLoadingRules()
//	overrides := &clientcmd.ConfigOverrides{}
//
//	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
//	if err != nil {
//		return nil, fmt.Errorf("Error building client config: %v", err)
//	}
//
//	return kubernetes.NewForConfig(config)
//}
//
//func createOrganizations(config Config, client *gitea.Client) error {
//	var err error
//	for _, org := range config.Organizations {
//		_, _, err = client.CreateOrg(gitea.CreateOrgOption{
//			Name:        org.Name,
//			Description: org.Description,
//		})
//
//		if err != nil {
//			log.Printf("Create organization %s: %v", org.Name, err)
//		}
//	}
//	return err
//}
//
//func createRepositories(config Config, client *gitea.Client) error {
//	var err error
//	for _, repo := range config.Repositories {
//		if repo.Migrate.Source != "" {
//			_, _, err = client.MigrateRepo(gitea.MigrateRepoOption{
//				RepoName:       repo.Name,
//				RepoOwner:      repo.Owner,
//				CloneAddr:      repo.Migrate.Source,
//				Service:        gitea.GitServicePlain,
//				Mirror:         repo.Migrate.Mirror,
//				Private:        repo.Private,
//				MirrorInterval: "10m",
//			})
//
//			if err != nil {
//				log.Printf("Migrate %s/%s: %v", repo.Owner, repo.Name, err)
//			}
//		} else {
//			_, _, err = client.AdminCreateRepo(repo.Owner, gitea.CreateRepoOption{
//				Name: repo.Name,
//				// Description: "TODO",
//				Private: repo.Private,
//			})
//		}
//	}
//	return err
//}
//
//func findExistingTokenValue(name string, client *gitea.Client) (string, error) {
//	var page = 0
//	for page >= 0 {
//		tokens, response, err := client.ListAccessTokens(gitea.ListAccessTokensOptions{
//			gitea.ListOptions{
//				PageSize: 50,
//				Page:     page,
//			},
//		})
//		for _, token := range tokens {
//			if token.Name == name {
//				return token.Token, nil
//			}
//		}
//		if err != nil {
//			return "", err
//		}
//		var link = response.Header.Get("link")
//		if s.Contains(link, "rel=\"next\"") {
//			page = page + 1
//		} else {
//			page = -1
//		}
//	}
//	return "", errors.New("token not found")
//}
//
//func createNewToken(name string, client *gitea.Client) (string, error) {
//	giteaToken, _, err := client.CreateAccessToken(gitea.CreateAccessTokenOption{
//		Name: name,
//	})
//	if err != nil {
//		return "", err
//	}
//	return giteaToken.Token, err
//}
//
//func storeToken(name string, value string) error {
//	client, err := getKubernetesClient()
//	if err != nil {
//		return fmt.Errorf("unable to create kubernetes client: %v", err)
//	}
//
//	secretData := map[string][]byte{}
//	secretData["token"] = []byte(value)
//	newSecret := &v1.Secret{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      fmt.Sprintf("gitea.tokens.%s	", name),
//			Namespace: secretsNamespace,
//		},
//		Data: secretData,
//	}
//
//	_, err = client.CoreV1().Secrets(secretsNamespace).Create(context.Background(), newSecret, metav1.CreateOptions{})
//	if err != nil {
//		return fmt.Errorf("unable to create secret: %v", err)
//	}
//	log.Printf("Secret '%s' created successfully.", name)
//
//	return nil
//}
//
//func createTokens(config Config, client *gitea.Client) error {
//	for _, token := range config.Tokens {
//		tokenValue, err := findExistingTokenValue(token.Name, client)
//		if err != nil && err.Error() == "token not found" {
//			tokenValue, err = createNewToken(token.Name, client)
//		}
//
//		err = storeToken(token.Name, tokenValue)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func waitUntilReady(client *kubernetes.Clientset) error {
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
//	defer cancel()
//
//	ticker := time.NewTicker(5 * time.Second)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-ctx.Done():
//			return fmt.Errorf("timeout waiting for gitea deployment to be ready")
//		case <-ticker.C:
//			deployments, err := client.AppsV1().Deployments("").List(ctx, metav1.ListOptions{
//				LabelSelector: "app=gitea",
//			})
//			if err != nil {
//				log.Printf("Error listing deployments: %v", err)
//				continue
//			}
//
//			if len(deployments.Items) == 0 {
//				log.Printf("No deployments found with label app=gitea")
//				continue
//			}
//
//			allReady := true
//			for _, deployment := range deployments.Items {
//				if !isDeploymentReady(&deployment) {
//					allReady = false
//					log.Printf("Deployment %s/%s is not ready yet", deployment.Namespace, deployment.Name)
//					break
//				}
//			}
//
//			if allReady {
//				log.Printf("All gitea deployments are ready")
//				return nil
//			}
//		}
//	}
//}
//
//func isDeploymentReady(deployment *appsv1.Deployment) bool {
//	if deployment.Status.Replicas == 0 {
//		return false
//	}
//	return deployment.Status.ReadyReplicas == deployment.Status.Replicas &&
//		deployment.Status.UpdatedReplicas == deployment.Status.Replicas &&
//		deployment.Status.AvailableReplicas == deployment.Status.Replicas
//}
//
//func main() {
//	_, err := os.ReadFile("./config.yaml")
//
//	if err != nil {
//		log.Fatalf("Unable to read config file: %v", err)
//	}
//
//	client, err := getKubernetesClient()
//	if err != nil {
//		log.Fatalf("Unable to create Kubernetes client: %v", err)
//	}
//
//	err = waitUntilReady(client)
//
//	//config := Config{}
//	//
//	//err = yaml.Unmarshal([]byte(data), &config)
//	//
//	//if err != nil {
//	//	log.Fatalf("error: %v", err)
//	//}
//	//
//	//gitea_host := os.Getenv("GITEA_HOST")
//	//gitea_user := os.Getenv("GITEA_USER")
//	//gitea_password := os.Getenv("GITEA_PASSWORD")
//	//
//	//options := (gitea.SetBasicAuth(gitea_user, gitea_password))
//	//client, err := gitea.NewClient(gitea_host, options)
//	//
//	//if err != nil {
//	//	log.Fatal(err)
//	//}
//	//
//	//err = createOrganizations(config, client)
//	//err = createRepositories(config, client)
//	//if err != nil {
//	//	log.Fatal(err)
//	//}
//	//
//	//err = createTokens(config, client)
//}
