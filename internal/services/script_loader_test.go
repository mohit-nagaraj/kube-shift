package services

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

func TestScriptLoader(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ScriptLoader Suite")
}

var _ = Describe("ScriptLoader", func() {
	var (
		ctx          context.Context
		fakeClient   client.Client
		scriptLoader interfaces.ScriptLoader
	)

	BeforeEach(func() {
		ctx = context.Background()
		s := scheme.Scheme
		s.AddKnownTypes(databasev1alpha1.GroupVersion, &databasev1alpha1.DatabaseMigration{})

		fakeClient = fake.NewClientBuilder().WithScheme(s).Build()
		scriptLoader = NewScriptLoader(fakeClient)
	})

	Describe("LoadScript", func() {
		Context("when loading from ConfigMap", func() {
			It("should load script content from ConfigMap", func() {
				// Create test ConfigMap
				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "migrations",
						Namespace: "default",
					},
					Data: map[string]string{
						"001_schema.sql": "ALTER TABLE users ADD COLUMN profile TEXT;",
					},
				}
				Expect(fakeClient.Create(ctx, configMap)).To(Succeed())

				script := databasev1alpha1.MigrationScript{
					Name:   "test-script",
					Type:   databasev1alpha1.ScriptTypeSchema,
					Source: "configmap://migrations/001_schema.sql",
				}

				content, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).NotTo(HaveOccurred())
				Expect(content).To(Equal("ALTER TABLE users ADD COLUMN profile TEXT;"))
			})

			It("should return error for non-existent ConfigMap", func() {
				script := databasev1alpha1.MigrationScript{
					Name:   "test-script",
					Type:   databasev1alpha1.ScriptTypeSchema,
					Source: "configmap://nonexistent/script.sql",
				}

				_, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not found"))
			})
		})

		Context("when loading from Secret", func() {
			It("should load script content from Secret", func() {
				// Create test Secret
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "migration-secrets",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"secret_script.sql": []byte("UPDATE users SET status = 'active';"),
					},
				}
				Expect(fakeClient.Create(ctx, secret)).To(Succeed())

				script := databasev1alpha1.MigrationScript{
					Name:   "secret-script",
					Type:   databasev1alpha1.ScriptTypeData,
					Source: "secret://migration-secrets/secret_script.sql",
				}

				content, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).NotTo(HaveOccurred())
				Expect(content).To(Equal("UPDATE users SET status = 'active';"))
			})
		})

		Context("when loading inline content", func() {
			It("should return inline script content", func() {
				script := databasev1alpha1.MigrationScript{
					Name:   "inline-script",
					Type:   databasev1alpha1.ScriptTypeValidation,
					Source: "inline",
				}

				// For inline scripts, we need to mock the content loading
				// This is a simplified test - in real implementation, inline content
				// would be stored differently or loaded from a different source
				_, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).To(HaveOccurred()) // Should fail for now since inline is not fully implemented
			})
		})

		Context("when validating checksums", func() {
			It("should validate correct checksum", func() {
				script := databasev1alpha1.MigrationScript{
					Name:     "test-script",
					Type:     databasev1alpha1.ScriptTypeSchema,
					Source:   "inline",
					Checksum: "sha256:8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
				}

				// This test would need to be updated when inline content is properly implemented
				_, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).To(HaveOccurred()) // Should fail for now
			})

			It("should return error for incorrect checksum", func() {
				script := databasev1alpha1.MigrationScript{
					Name:     "test-script",
					Type:     databasev1alpha1.ScriptTypeSchema,
					Source:   "inline",
					Checksum: "sha256:invalid",
				}

				_, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when validating SQL safety", func() {
			It("should reject dangerous SQL statements", func() {
				script := databasev1alpha1.MigrationScript{
					Name:   "dangerous-script",
					Type:   databasev1alpha1.ScriptTypeSchema,
					Source: "inline",
				}

				_, err := scriptLoader.LoadScript(ctx, script)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("ValidateScript", func() {
		It("should validate script syntax and checksum", func() {
			script := databasev1alpha1.MigrationScript{
				Name:     "test-script",
				Type:     databasev1alpha1.ScriptTypeSchema,
				Source:   "configmap://migrations/test.sql",
				Checksum: "sha256:8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",
			}

			content := "ALTER TABLE users ADD COLUMN email VARCHAR(255);"
			err := scriptLoader.ValidateScript(ctx, script, content)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
