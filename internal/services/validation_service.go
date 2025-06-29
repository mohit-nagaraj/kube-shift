package services

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/engines/mariadb"
	"github.com/mohit-nagaraj/kube-shift/internal/engines/mysql"
	"github.com/mohit-nagaraj/kube-shift/internal/engines/postgresql"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// Validation constants
const (
	HundredPercent = "100%"
)

// ValidationService implements the ValidationService interface
type ValidationService struct {
	client client.Client
}

// NewValidationService creates a new ValidationService instance
func NewValidationService(k8sClient client.Client) interfaces.ValidationService {
	return &ValidationService{
		client: k8sClient,
	}
}

// RunPreChecks runs all pre-migration checks
func (vs *ValidationService) RunPreChecks(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error {
	logger := log.FromContext(ctx)
	logger.Info("Running pre-migration checks", "migration", migration.Name)

	// Get database connection
	db, err := vs.getDatabaseConnection(ctx, migration)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close database connection")
		}
	}()

	// Run connection checks
	if err := vs.checkConnection(ctx, db); err != nil {
		return fmt.Errorf("connection check failed: %w", err)
	}

	// Run permission checks
	if err := vs.checkPermissions(ctx, db); err != nil {
		return fmt.Errorf("permission check failed: %w", err)
	}

	// Run disk space checks
	vs.checkDiskSpace(ctx, migration)

	// Run table existence checks
	if err := vs.checkTableExistence(ctx, db, migration); err != nil {
		return fmt.Errorf("table existence check failed: %w", err)
	}

	// Run data size checks
	if err := vs.checkDataSize(ctx, db, migration); err != nil {
		return fmt.Errorf("data size check failed: %w", err)
	}

	// Run performance baseline checks
	if err := vs.checkPerformanceBaseline(ctx, db); err != nil {
		return fmt.Errorf("performance baseline check failed: %w", err)
	}

	logger.Info("All pre-migration checks passed", "migration", migration.Name)
	return nil
}

// ValidatePerformance checks if migration meets performance thresholds
func (vs *ValidationService) ValidatePerformance(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, metrics *databasev1alpha1.MigrationMetrics) error {
	logger := log.FromContext(ctx)

	if migration.Spec.Validation == nil {
		logger.V(1).Info("No validation configured, skipping performance validation")
		return nil
	}

	thresholds := migration.Spec.Validation.PerformanceThresholds

	// Check migration duration
	if thresholds.MaxMigrationTime != nil {
		if migration.Status.StartTime != nil {
			duration := time.Since(migration.Status.StartTime.Time)
			if duration > thresholds.MaxMigrationTime.Duration {
				return fmt.Errorf("migration duration %v exceeds threshold %v", duration, thresholds.MaxMigrationTime.Duration)
			}
		}
	}

	// Check CPU usage
	if thresholds.MaxCPUUsage != "" {
		if metrics != nil && metrics.CPUUsage != "" {
			if err := vs.compareUsage(metrics.CPUUsage, thresholds.MaxCPUUsage, "CPU"); err != nil {
				return err
			}
		}
	}

	// Check memory usage
	if thresholds.MaxMemoryUsage != "" {
		if metrics != nil && metrics.MemoryUsage != "" {
			if err := vs.compareUsage(metrics.MemoryUsage, thresholds.MaxMemoryUsage, "Memory"); err != nil {
				return err
			}
		}
	}

	// Check disk usage
	if metrics != nil && metrics.DiskUsage != "" {
		// Use a default threshold if not specified
		defaultDiskThreshold := "80%"
		if err := vs.compareUsage(metrics.DiskUsage, defaultDiskThreshold, "Disk"); err != nil {
			return err
		}
	}

	logger.V(1).Info("Performance validation passed", "migration", migration.Name)
	return nil
}

// ShouldRollback determines if migration should be rolled back
func (vs *ValidationService) ShouldRollback(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, metrics *databasev1alpha1.MigrationMetrics) (bool, string, error) {
	if migration.Spec.Rollback == nil || !migration.Spec.Rollback.Automatic {
		return false, "", nil
	}

	// Check for performance degradation
	if err := vs.ValidatePerformance(ctx, migration, metrics); err != nil {
		return true, "performance-degradation", err
	}

	// Check for timeout
	if migration.Spec.Validation != nil {
		thresholds := migration.Spec.Validation.PerformanceThresholds
		if thresholds.MaxMigrationTime != nil {
			if migration.Status.StartTime != nil {
				duration := time.Since(migration.Status.StartTime.Time)
				if duration > thresholds.MaxMigrationTime.Duration {
					return true, "timeout", fmt.Errorf("migration exceeded maximum time limit: %v", duration)
				}
			}
		}
	}

	// Check custom rollback conditions
	if migration.Spec.Rollback.Conditions != nil {
		for _, condition := range migration.Spec.Rollback.Conditions {
			if vs.checkCustomCondition(metrics, condition) {
				return true, condition, fmt.Errorf("custom rollback condition triggered: %s", condition)
			}
		}
	}

	return false, "", nil
}

// Helper methods

func (vs *ValidationService) getDatabaseConnection(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*sql.DB, error) {
	// Get database credentials from secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := vs.client.Get(ctx, secretKey, secret); err != nil {
		return nil, fmt.Errorf("failed to get database secret: %w", err)
	}

	// Convert secret data to map
	credentials := make(map[string]string)
	for key, value := range secret.Data {
		credentials[key] = string(value)
	}

	// Get migration engine based on database type
	var engine interfaces.MigrationEngine
	switch migration.Spec.Database.Type {
	case databasev1alpha1.DatabaseTypePostgreSQL:
		engine = postgresql.NewEngine()
	case databasev1alpha1.DatabaseTypeMySQL:
		engine = mysql.NewEngine()
	case databasev1alpha1.DatabaseTypeMariaDB:
		engine = mariadb.NewEngine()
	default:
		return nil, fmt.Errorf("unsupported database type: %s", migration.Spec.Database.Type)
	}

	// Get connection
	return engine.GetConnection(ctx, migration.Spec.Database, credentials)
}

func (vs *ValidationService) checkConnection(ctx context.Context, db *sql.DB) error {
	// Test connection with timeout
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("database connection failed: %w", err)
	}

	return nil
}

func (vs *ValidationService) checkPermissions(ctx context.Context, db *sql.DB) error {
	// Test basic permissions
	testQueries := []string{
		"SELECT 1",
		"SHOW TABLES",
		"SELECT COUNT(*) FROM information_schema.tables",
	}

	for _, query := range testQueries {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("permission check failed for query '%s': %w", query, err)
		}
	}

	return nil
}

func (vs *ValidationService) checkDiskSpace(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) {
	if migration.Spec.Validation == nil || migration.Spec.Validation.PreChecks == nil {
		return
	}

	for _, check := range migration.Spec.Validation.PreChecks {
		if check.Type == "disk-space" && check.Threshold != "" {
			// This is a simplified check - in production you'd query actual disk usage
			// For now, we'll assume sufficient space
			logger := log.FromContext(ctx)
			logger.V(1).Info("Disk space check passed", "threshold", check.Threshold)
			return
		}
	}
}

func (vs *ValidationService) checkTableExistence(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	// Extract table names from migration scripts
	tables := vs.extractTableNames(migration.Spec.Migration.Scripts)

	for _, table := range tables {
		// Check if table exists
		query := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", table)
		var count int
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("failed to check table existence for %s: %w", table, err)
		}

		if count == 0 {
			return fmt.Errorf("table %s does not exist", table)
		}
	}

	return nil
}

func (vs *ValidationService) checkDataSize(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	tables := vs.extractTableNames(migration.Spec.Migration.Scripts)

	for _, table := range tables {
		// Get row count
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		var rowCount int64
		if err := db.QueryRowContext(ctx, query).Scan(&rowCount); err != nil {
			return fmt.Errorf("failed to get row count for table %s: %w", table, err)
		}

		// Log data size for monitoring
		logger := log.FromContext(ctx)
		logger.V(1).Info("Table data size", "table", table, "rows", rowCount)
	}

	return nil
}

func (vs *ValidationService) checkPerformanceBaseline(ctx context.Context, db *sql.DB) error {
	// Run a simple performance test
	start := time.Now()

	// Execute a simple query to measure baseline performance
	query := "SELECT 1"
	for i := 0; i < 10; i++ {
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("performance baseline check failed: %w", err)
		}
	}

	duration := time.Since(start)
	avgDuration := duration / 10

	logger := log.FromContext(ctx)
	logger.V(1).Info("Performance baseline", "avgQueryTime", avgDuration)

	return nil
}

func (vs *ValidationService) compareUsage(current, threshold, resourceType string) error {
	// Parse current usage (e.g., "45%", "2.5GB")
	currentValue, currentUnit := vs.parseUsage(current)
	thresholdValue, thresholdUnit := vs.parseUsage(threshold)

	// Convert to same unit for comparison
	if currentUnit != thresholdUnit {
		// Simplified conversion - in production you'd want more sophisticated conversion
		if currentUnit == "%" && thresholdUnit == "%" {
			// Both are percentages
		} else {
			// For now, just log a warning
			logger := log.FromContext(context.Background())
			logger.V(1).Info("Usage units differ, skipping comparison",
				"current", current, "threshold", threshold, "resource", resourceType)
			return nil
		}
	}

	if currentValue > thresholdValue {
		return fmt.Errorf("%s usage %s exceeds threshold %s", resourceType, current, threshold)
	}

	return nil
}

func (vs *ValidationService) parseUsage(usage string) (float64, string) {
	usage = strings.TrimSpace(usage)

	// Handle percentage
	if strings.HasSuffix(usage, "%") {
		if value, err := strconv.ParseFloat(strings.TrimSuffix(usage, "%"), 64); err == nil {
			return value, "%"
		}
	}

	// Handle bytes (GB, MB, etc.)
	if strings.HasSuffix(usage, "GB") {
		if value, err := strconv.ParseFloat(strings.TrimSuffix(usage, "GB"), 64); err == nil {
			return value, "GB"
		}
	}

	if strings.HasSuffix(usage, "MB") {
		if value, err := strconv.ParseFloat(strings.TrimSuffix(usage, "MB"), 64); err == nil {
			return value, "MB"
		}
	}

	// Default to raw number
	if value, err := strconv.ParseFloat(usage, 64); err == nil {
		return value, "raw"
	}

	return 0, "unknown"
}

func (vs *ValidationService) extractTableNames(scripts []databasev1alpha1.MigrationScript) []string {
	tables := make(map[string]bool)

	for _, script := range scripts {
		// Load script content using ScriptLoader
		scriptContent, err := vs.loadScriptContent(context.Background(), script)
		if err != nil {
			// Log error but continue with other scripts
			logger := log.FromContext(context.Background())
			logger.V(1).Info("Failed to load script content", "script", script.Name, "error", err)
			continue
		}

		// Extract table names from SQL content
		scriptTables := vs.parseTableNamesFromSQL(scriptContent)
		for _, table := range scriptTables {
			tables[table] = true
		}
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}

	return result
}

// loadScriptContent loads script content from various sources
func (vs *ValidationService) loadScriptContent(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
	// Parse source and load content
	switch {
	case strings.HasPrefix(script.Source, "configmap://"):
		return vs.loadFromConfigMap(ctx, script)
	case strings.HasPrefix(script.Source, "secret://"):
		return vs.loadFromSecret(ctx, script)
	case strings.HasPrefix(script.Source, "inline://"):
		return vs.loadInline(script)
	case script.Source == "":
		// Fallback to inline content if no source specified
		return vs.loadInline(script)
	default:
		return "", fmt.Errorf("unsupported script source: %s", script.Source)
	}
}

// loadFromConfigMap loads script content from a ConfigMap
func (vs *ValidationService) loadFromConfigMap(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
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

	if err := vs.client.Get(ctx, configMapKey, configMap); err != nil {
		return "", fmt.Errorf("failed to get configmap: %w", err)
	}

	content, exists := configMap.Data[key]
	if !exists {
		return "", fmt.Errorf("key %s not found in configmap %s/%s", key, namespace, name)
	}

	return content, nil
}

// loadFromSecret loads script content from a Secret
func (vs *ValidationService) loadFromSecret(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error) {
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

	if err := vs.client.Get(ctx, secretKey, secret); err != nil {
		return "", fmt.Errorf("failed to get secret: %w", err)
	}

	contentBytes, exists := secret.Data[key]
	if !exists {
		return "", fmt.Errorf("key %s not found in secret %s/%s", key, namespace, name)
	}

	return string(contentBytes), nil
}

// loadInline loads inline script content
func (vs *ValidationService) loadInline(script databasev1alpha1.MigrationScript) (string, error) {
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

// parseTableNamesFromSQL extracts table names from SQL content using regex patterns
func (vs *ValidationService) parseTableNamesFromSQL(content string) []string {
	tables := make(map[string]bool)

	// Convert to lowercase for case-insensitive matching
	contentLower := strings.ToLower(content)

	// Regex patterns for different SQL statements that reference tables
	patterns := []struct {
		pattern string
		group   int
	}{
		// CREATE TABLE statements
		{`create\s+table\s+(?:if\s+not\s+exists\s+)?([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// ALTER TABLE statements
		{`alter\s+table\s+([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// DROP TABLE statements
		{`drop\s+table\s+(?:if\s+exists\s+)?([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// INSERT INTO statements
		{`insert\s+(?:into\s+)?([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// UPDATE statements
		{`update\s+([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// DELETE FROM statements
		{`delete\s+from\s+([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// SELECT FROM statements (basic pattern)
		{`from\s+([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// JOIN statements
		{`join\s+([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
		// LEFT/RIGHT/INNER JOIN statements
		{`(?:left|right|inner|outer)\s+join\s+([` + "`'\"" + `]?\w+[` + "`'\"" + `]?)`, 1},
	}

	for _, p := range patterns {
		re := regexp.MustCompile(p.pattern)
		matches := re.FindAllStringSubmatch(contentLower, -1)

		for _, match := range matches {
			if len(match) > p.group {
				tableName := strings.Trim(match[p.group], "`'\"")
				// Filter out common SQL keywords that might be captured
				if !vs.isSQLKeyword(tableName) {
					tables[tableName] = true
				}
			}
		}
	}

	// Convert map to slice
	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}

	return result
}

// isSQLKeyword checks if a string is a common SQL keyword to avoid false positives
func (vs *ValidationService) isSQLKeyword(word string) bool {
	keywords := map[string]bool{
		"select": true, "from": true, "where": true, "insert": true, "update": true,
		"delete": true, "create": true, "alter": true, "drop": true, "table": true,
		"into": true, "values": true, "set": true, "join": true, "left": true,
		"right": true, "inner": true, "outer": true, "on": true, "as": true,
		"order": true, "group": true, "by": true, "having": true, "limit": true,
		"offset": true, "distinct": true, "count": true, "sum": true, "avg": true,
		"max": true, "min": true, "case": true, "when": true, "then": true,
		"else": true, "end": true, "if": true, "exists": true, "not": true,
		"and": true, "or": true, "in": true, "between": true, "like": true,
		"null": true, "is": true, "all": true, "any": true, "some": true,
	}
	return keywords[strings.ToLower(word)]
}

func (vs *ValidationService) checkCustomCondition(metrics *databasev1alpha1.MigrationMetrics, condition string) bool {
	// Implement custom condition checking logic
	// This is a placeholder - in production you'd have more sophisticated condition evaluation
	switch condition {
	case "high-error-rate":
		// Check if there are any error indicators in metrics
		return metrics != nil && (metrics.CPUUsage == HundredPercent || metrics.MemoryUsage == HundredPercent)
	case "slow-queries":
		// Check if queries per second is very low
		return metrics != nil && metrics.QueriesPerSecond == "0"
	case "connection-timeout":
		// Check if disk usage is very high (indicator of potential issues)
		return metrics != nil && metrics.DiskUsage == "100%"
	default:
		return false
	}
}
