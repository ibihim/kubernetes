/*
Copyright 2025 The Kubernetes Authors.

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

package apimachinery

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eauth "k8s.io/kubernetes/test/e2e/framework/auth"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"
)

const (
	// encryptionConfigPath is the path to the encryption config on the control plane node.
	encryptionConfigPath = "/etc/kubernetes/encryption-config.yaml"

	// corruptedEncryptionConfig uses a different encryption provider (aescbc) than the
	// original config (aesgcm), making secrets encrypted with the original config unreadable.
	corruptedEncryptionConfig = `apiVersion: apiserver.config.k8s.io/v1
kind: EncryptionConfiguration
resources:
  - resources:
    - secrets
    providers:
    - aescbc:
        keys:
        - name: key2
          secret: YW5vdGhlciBzZWNyZXQga2V5IQ==
    - identity: {}`

)

var _ = SIGDescribe("Corrupt object deletion", feature.AllowUnsafeMalformedObjectDeletion, framework.WithDisruptive(), framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("corrupt-object-deletion")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	var (
		controlPlaneNodeName string
		originalConfig       string
	)

	ginkgo.BeforeEach(func(ctx context.Context) {
		// This test requires a Kind cluster with specific encryption configuration.
		// It uses docker exec to modify the encryption config on the control plane node.
		if !isKindCluster() {
			ginkgo.Skip("Test requires Kind cluster with docker access")
		}

		// Get control plane node name
		nodes := framework.GetControlPlaneNodes(ctx, f.ClientSet)
		gomega.Expect(nodes.Items).NotTo(gomega.BeEmpty(),
			"at least one node with label %s should exist", framework.ControlPlaneLabel)
		controlPlaneNodeName = nodes.Items[0].Name

		// Save original encryption config for restoration
		var err error
		originalConfig, err = readFileFromKindNode(controlPlaneNodeName, encryptionConfigPath)
		framework.ExpectNoError(err, "failed to read original encryption config")
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if originalConfig != "" && controlPlaneNodeName != "" {
			ginkgo.By("Restoring original encryption config")
			err := writeFileToKindNode(controlPlaneNodeName, encryptionConfigPath, originalConfig)
			if err != nil {
				framework.Logf("Warning: failed to restore encryption config: %v", err)
			}
			// Wait for the config to be reloaded
			time.Sleep(10 * time.Second)
		}
	})

	/*
		Release: v1.33
		Testname: Corrupt object deletion with IgnoreStoreReadErrorWithClusterBreakingPotential
		Description: This test verifies that corrupt/undecryptable objects can be deleted using
		the IgnoreStoreReadErrorWithClusterBreakingPotential delete option when the
		AllowUnsafeMalformedObjectDeletion feature gate is enabled.
		The test:
		1. Creates a secret encrypted with a specific key
		2. Corrupts the encryption config to use a different key/provider
		3. Verifies the secret becomes unreadable (InternalError)
		4. Verifies normal delete fails with InternalError
		5. Verifies unsafe delete without permission fails with Forbidden
		6. Grants unsafe-delete-ignore-read-errors permission
		7. Verifies unsafe delete with permission succeeds
	*/
	ginkgo.It("should delete a corrupt secret using IgnoreStoreReadErrorWithClusterBreakingPotential", func(ctx context.Context) {
		secretName := "test-corrupt-secret"
		testUser := "kep3926-test-user"

		ginkgo.By("Setting up test user with basic permissions (without unsafe-delete)")
		adminClient := f.ClientSet
		setupRBACForTestUser(ctx, adminClient, testUser, f.Namespace.Name, []string{"create", "get", "delete"})

		// Create impersonated client for test user
		testUserConfig := rest.CopyConfig(f.ClientConfig())
		testUserConfig.Impersonate.UserName = testUser
		testUserClient, err := clientset.NewForConfig(testUserConfig)
		framework.ExpectNoError(err, "failed to create test user client")

		ginkgo.By("Creating a secret as test user")
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: f.Namespace.Name,
			},
			Data: map[string][]byte{
				"key": []byte("value"),
			},
		}
		_, err = testUserClient.CoreV1().Secrets(f.Namespace.Name).Create(ctx, secret, metav1.CreateOptions{})
		framework.ExpectNoError(err, "failed to create secret")

		ginkgo.By("Verifying secret is readable before corruption")
		_, err = testUserClient.CoreV1().Secrets(f.Namespace.Name).Get(ctx, secretName, metav1.GetOptions{})
		framework.ExpectNoError(err, "secret should be readable before corruption")

		ginkgo.By("Corrupting the secret by changing encryption config")
		err = writeFileToKindNode(controlPlaneNodeName, encryptionConfigPath, corruptedEncryptionConfig)
		framework.ExpectNoError(err, "failed to write corrupted encryption config")

		ginkgo.By("Waiting for encryption config to reload and corruption to take effect")
		err = waitForSecretCorruption(ctx, testUserClient, f.Namespace.Name, secretName)
		framework.ExpectNoError(err, "secret should become corrupt")

		ginkgo.By("Attempting normal delete (should fail with InternalError)")
		err = testUserClient.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, secretName, metav1.DeleteOptions{})
		gomega.Expect(apierrors.IsInternalError(err)).To(gomega.BeTrue(),
			"normal delete should fail with InternalError for corrupt object, got: %v", err)

		ginkgo.By("Attempting unsafe delete WITHOUT permission (should fail with Forbidden)")
		unsafeDeleteOpts := metav1.DeleteOptions{
			IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true),
		}
		err = testUserClient.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, secretName, unsafeDeleteOpts)
		gomega.Expect(apierrors.IsForbidden(err)).To(gomega.BeTrue(),
			"unsafe delete without permission should be Forbidden, got: %v", err)
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("unsafe-delete-ignore-read-errors"),
			"error should mention the missing verb")

		ginkgo.By("Granting unsafe-delete-ignore-read-errors permission to test user")
		setupRBACForTestUser(ctx, adminClient, testUser, f.Namespace.Name, []string{"unsafe-delete-ignore-read-errors"})

		// Wait for RBAC to propagate
		err = e2eauth.WaitForNamedAuthorizationUpdate(ctx, adminClient.AuthorizationV1(),
			testUser, f.Namespace.Name, "unsafe-delete-ignore-read-errors", "",
			schema.GroupResource{Group: "", Resource: "secrets"}, true)
		framework.ExpectNoError(err, "failed waiting for RBAC to propagate")

		ginkgo.By("Attempting unsafe delete WITH permission (should succeed)")
		err = testUserClient.CoreV1().Secrets(f.Namespace.Name).Delete(ctx, secretName, unsafeDeleteOpts)
		framework.ExpectNoError(err, "unsafe delete with permission should succeed")

		ginkgo.By("Verifying secret is deleted")
		_, err = adminClient.CoreV1().Secrets(f.Namespace.Name).Get(ctx, secretName, metav1.GetOptions{})
		gomega.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue(),
			"secret should not exist after deletion")
	})
})

// isKindCluster checks if we're running on a Kind cluster by looking for the kind container.
func isKindCluster() bool {
	// Check if docker is available and kind cluster containers exist
	cmd := exec.Command("docker", "ps", "--format", "{{.Names}}", "--filter", "name=-control-plane")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(output), "control-plane")
}

// readFileFromKindNode reads a file from a Kind node using docker exec.
func readFileFromKindNode(nodeName, filePath string) (string, error) {
	cmd := exec.Command("docker", "exec", nodeName, "cat", filePath)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to read %s from node %s: %w", filePath, nodeName, err)
	}
	return string(output), nil
}

// writeFileToKindNode writes content to a file on a Kind node using docker exec.
func writeFileToKindNode(nodeName, filePath, content string) error {
	// Use bash -c with heredoc to write the file
	cmd := exec.Command("docker", "exec", "-i", nodeName, "bash", "-c",
		fmt.Sprintf("cat > %s", filePath))
	cmd.Stdin = strings.NewReader(content)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to write %s on node %s: %w (output: %s)", filePath, nodeName, err, string(output))
	}
	return nil
}

// waitForSecretCorruption polls until the secret returns an InternalError (indicating corruption).
func waitForSecretCorruption(ctx context.Context, client clientset.Interface, namespace, secretName string) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, 2*time.Minute, true,
		func(ctx context.Context) (bool, error) {
			_, err := client.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
			if err == nil {
				framework.Logf("Secret still readable, waiting for encryption config reload...")
				return false, nil // Not corrupt yet
			}
			if apierrors.IsInternalError(err) {
				framework.Logf("Secret is now corrupt (InternalError): %v", err)
				return true, nil
			}
			// Unexpected error type
			framework.Logf("Unexpected error reading secret: %v", err)
			return false, nil
		})
}

// setupRBACForTestUser creates a Role and RoleBinding granting the specified verbs on secrets.
func setupRBACForTestUser(ctx context.Context, client clientset.Interface, user, namespace string, verbs []string) {
	roleName := fmt.Sprintf("%s-secrets-%s", user, strings.Join(verbs, "-"))

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:     verbs,
				APIGroups: []string{""},
				Resources: []string{"secrets"},
			},
		},
	}

	_, err := client.RbacV1().Roles(namespace).Create(ctx, role, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		framework.ExpectNoError(err, "failed to create role %s", roleName)
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      roleName,
			Namespace: namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:     rbacv1.UserKind,
				Name:     user,
				APIGroup: rbacv1.GroupName,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     roleName,
		},
	}

	_, err = client.RbacV1().RoleBindings(namespace).Create(ctx, binding, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		framework.ExpectNoError(err, "failed to create role binding %s", roleName)
	}
}
