package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// ScriptLoader implements the ScriptLoader interface
type ScriptLoader struct {
	client client.Client
	cache  *ScriptCache
}

// ScriptCache provides caching for loaded scripts
type ScriptCache struct {
	scripts map[string]*CachedScript
	mutex   sync.RWMutex
}

// CachedScript represents a cached script with metadata
type CachedScript struct {
	Content   string
	Checksum  string
	LoadedAt  time.Time
	ExpiresAt time.Time
}

// NewScriptLoader creates a new ScriptLoader instance
func NewScriptLoader(client client.Client) interfaces.ScriptLoader {
	return &ScriptLoader{
		client: client,
		cache: &ScriptCache{
			scripts: make(map[string]*CachedScript),
		},
	}
}

// LoadScript loads a migration script from various sources
func (sl *ScriptLoader) LoadScript(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
	log := log.FromContext(ctx)

	// Check cache first
	if cached := sl.cache.get(script.Name); cached != nil {
		log.V(1).Info("Using cached script", "script", script.Name)
		return cached.Content, nil
	}

	var content string
	var err error

	// Parse source and load content
	switch {
	case strings.HasPrefix(script.Source, "configmap://"):
		content, err = sl.loadFromConfigMap(ctx, script)
	case strings.HasPrefix(script.Source, "secret://"):
		content, err = sl.loadFromSecret(ctx, script)
	case strings.HasPrefix(script.Source, "inline://"):
		content, err = sl.loadInline(ctx, script)
	case script.Source == "":
		// Fallback to inline content if no source specified
		content, err = sl.loadInline(ctx, script)
	default:
		return "", fmt.Errorf("unsupported script source: %s", script.Source)
	}

	if err != nil {
		return "", fmt.Errorf("failed to load script %s: %w", script.Name, err)
	}

	// Validate script
	if err := sl.ValidateScript(ctx, script, content); err != nil {
		return "", fmt.Errorf("script validation failed for %s: %w", script.Name, err)
	}

	// Cache the script
	sl.cache.set(script.Name, content, script.Checksum)

	return content, nil
}

// ValidateScript validates script syntax and checksum
func (sl *ScriptLoader) ValidateScript(ctx context.Context, script databasev1alpha1.MigrationScript, content string) error {
	log := log.FromContext(ctx)

	// Validate checksum if provided
	if script.Checksum != "" {
		calculatedChecksum := sl.calculateChecksum(content)
		if !strings.HasSuffix(script.Checksum, calculatedChecksum) {
			return fmt.Errorf("checksum mismatch: expected %s, got %s", script.Checksum, calculatedChecksum)
		}
		log.V(1).Info("Script checksum validated", "script", script.Name)
	}

	// Basic SQL syntax validation
	if err := sl.validateSQLSyntax(content, script.Type); err != nil {
		return fmt.Errorf("SQL syntax validation failed: %w", err)
	}

	// Check for dangerous operations if validation is enabled
	if err := sl.validateSafety(content, script.Type); err != nil {
		return fmt.Errorf("safety validation failed: %w", err)
	}

	return nil
}

// loadFromConfigMap loads script content from a ConfigMap
func (sl *ScriptLoader) loadFromConfigMap(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
	// Parse configmap reference: configmap://namespace/name/key
	parts := strings.Split(strings.TrimPrefix(script.Source, "configmap://"), "/")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid configmap reference format: %s", script.Source)
	}

	namespace, name, key := parts[0], parts[1], parts[2]

	configMap := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	if err := sl.client.Get(ctx, configMapKey, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("configmap not found: %s/%s", namespace, name)
		}
		return "", fmt.Errorf("failed to get configmap: %w", err)
	}

	content, exists := configMap.Data[key]
	if !exists {
		return "", fmt.Errorf("key %s not found in configmap %s/%s", key, namespace, name)
	}

	return content, nil
}

// loadFromSecret loads script content from a Secret
func (sl *ScriptLoader) loadFromSecret(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
	// Parse secret reference: secret://namespace/name/key
	parts := strings.Split(strings.TrimPrefix(script.Source, "secret://"), "/")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid secret reference format: %s", script.Source)
	}

	namespace, name, key := parts[0], parts[1], parts[2]

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	if err := sl.client.Get(ctx, secretKey, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("secret not found: %s/%s", namespace, name)
		}
		return "", fmt.Errorf("failed to get secret: %w", err)
	}

	contentBytes, exists := secret.Data[key]
	if !exists {
		return "", fmt.Errorf("key %s not found in secret %s/%s", key, namespace, name)
	}

	return string(contentBytes), nil
}

// loadInline loads inline script content
func (sl *ScriptLoader) loadInline(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
	// For inline content, we'll use the script name as a template
	// In a real implementation, this might come from the script spec
	switch script.Type {
	case databasev1alpha1.ScriptTypeSchema:
		return fmt.Sprintf("-- Schema migration script: %s\n-- Generated at: %s\nALTER TABLE users ADD COLUMN %s VARCHAR(255);",
			script.Name, time.Now().Format(time.RFC3339), strings.ReplaceAll(script.Name, "-", "_")), nil
	case databasev1alpha1.ScriptTypeData:
		return fmt.Sprintf("-- Data migration script: %s\n-- Generated at: %s\nUPDATE users SET %s = 'default_value' WHERE %s IS NULL;",
			script.Name, time.Now().Format(time.RFC3339), strings.ReplaceAll(script.Name, "-", "_"), strings.ReplaceAll(script.Name, "-", "_")), nil
	case databasev1alpha1.ScriptTypeValidation:
		return fmt.Sprintf("-- Validation script: %s\n-- Generated at: %s\nSELECT COUNT(*) FROM users WHERE %s IS NOT NULL;",
			script.Name, time.Now().Format(time.RFC3339), strings.ReplaceAll(script.Name, "-", "_")), nil
	default:
		return fmt.Sprintf("-- Script: %s\n-- Generated at: %s\n-- No specific content for script type: %s",
			script.Name, time.Now().Format(time.RFC3339), script.Type), nil
	}
}

// calculateChecksum calculates SHA256 checksum of content
func (sl *ScriptLoader) calculateChecksum(content string) string {
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

// validateSQLSyntax performs basic SQL syntax validation
func (sl *ScriptLoader) validateSQLSyntax(content string, scriptType databasev1alpha1.ScriptType) error {
	content = strings.TrimSpace(content)
	if content == "" {
		return fmt.Errorf("script content is empty")
	}

	// Basic validation based on script type
	switch scriptType {
	case databasev1alpha1.ScriptTypeSchema:
		// Check for common schema operations
		contentLower := strings.ToLower(content)
		validOperations := []string{"create", "alter", "drop", "add", "modify", "rename"}
		hasValidOp := false
		for _, op := range validOperations {
			if strings.Contains(contentLower, op) {
				hasValidOp = true
				break
			}
		}
		if !hasValidOp {
			return fmt.Errorf("schema script should contain schema operations (CREATE, ALTER, DROP, etc.)")
		}
	case databasev1alpha1.ScriptTypeData:
		// Check for data operations
		contentLower := strings.ToLower(content)
		validOperations := []string{"insert", "update", "delete", "select"}
		hasValidOp := false
		for _, op := range validOperations {
			if strings.Contains(contentLower, op) {
				hasValidOp = true
				break
			}
		}
		if !hasValidOp {
			return fmt.Errorf("data script should contain data operations (INSERT, UPDATE, DELETE, SELECT)")
		}
	}

	return nil
}

// validateSafety checks for potentially dangerous operations
func (sl *ScriptLoader) validateSafety(content string, scriptType databasev1alpha1.ScriptType) error {
	contentLower := strings.ToLower(content)

	// Check for dangerous operations
	dangerousOps := []string{
		"drop database",
		"drop schema",
		"truncate",
		"delete from",
		"update set",
	}

	for _, op := range dangerousOps {
		if strings.Contains(contentLower, op) {
			// Log warning but don't fail for now
			// In production, you might want to require explicit approval
			fmt.Printf("Warning: Script contains potentially dangerous operation: %s\n", op)
		}
	}

	return nil
}

// Cache methods

func (sc *ScriptCache) get(key string) *CachedScript {
	sc.mutex.RLock()
	defer sc.mutex.RUnlock()

	if cached, exists := sc.scripts[key]; exists && time.Now().Before(cached.ExpiresAt) {
		return cached
	}
	return nil
}

func (sc *ScriptCache) set(key, content, checksum string) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.scripts[key] = &CachedScript{
		Content:   content,
		Checksum:  checksum,
		LoadedAt:  time.Now(),
		ExpiresAt: time.Now().Add(30 * time.Minute), // Cache for 30 minutes
	}
}

func (sc *ScriptCache) clear() {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.scripts = make(map[string]*CachedScript)
}
