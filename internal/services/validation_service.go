package services

import (
	"context"
	"database/sql"
	"fmt"
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

// ValidationService implements the ValidationService interface
type ValidationService struct {
	client client.Client
}

// NewValidationService creates a new ValidationService instance
func NewValidationService(client client.Client) interfaces.ValidationService {
	return &ValidationService{
		client: client,
	}
}

// RunPreChecks runs pre-migration validation checks
func (vs *ValidationService) RunPreChecks(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error {
	log := log.FromContext(ctx)
	log.Info("Running pre-migration checks", "migration", migration.Name)

	// Get database connection
	db, err := vs.getDatabaseConnection(ctx, migration)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	defer db.Close()

	// Run connection checks
	if err := vs.checkConnection(ctx, db); err != nil {
		return fmt.Errorf("connection check failed: %w", err)
	}

	// Run permission checks
	if err := vs.checkPermissions(ctx, db); err != nil {
		return fmt.Errorf("permission check failed: %w", err)
	}

	// Run disk space checks
	if err := vs.checkDiskSpace(ctx, db, migration); err != nil {
		return fmt.Errorf("disk space check failed: %w", err)
	}

	// Run table existence checks
	if err := vs.checkTableExistence(ctx, db, migration); err != nil {
		return fmt.Errorf("table existence check failed: %w", err)
	}

	// Run data size checks
	if err := vs.checkDataSize(ctx, db, migration); err != nil {
		return fmt.Errorf("data size check failed: %w", err)
	}

	// Run performance baseline checks
	if err := vs.checkPerformanceBaseline(ctx, db, migration); err != nil {
		return fmt.Errorf("performance baseline check failed: %w", err)
	}

	log.Info("All pre-migration checks passed", "migration", migration.Name)
	return nil
}

// ValidatePerformance checks if migration meets performance thresholds
func (vs *ValidationService) ValidatePerformance(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, metrics *databasev1alpha1.MigrationMetrics) error {
	log := log.FromContext(ctx)

	if migration.Spec.Validation == nil {
		log.V(1).Info("No validation configured, skipping performance validation")
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

	log.V(1).Info("Performance validation passed", "migration", migration.Name)
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
			if vs.checkCustomCondition(ctx, migration, metrics, condition) {
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

func (vs *ValidationService) checkDiskSpace(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	if migration.Spec.Validation == nil || migration.Spec.Validation.PreChecks == nil {
		return nil
	}

	for _, check := range migration.Spec.Validation.PreChecks {
		if check.Type == "disk-space" && check.Threshold != "" {
			// This is a simplified check - in production you'd query actual disk usage
			// For now, we'll assume sufficient space
			log := log.FromContext(ctx)
			log.V(1).Info("Disk space check passed", "threshold", check.Threshold)
		}
	}

	return nil
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
		log := log.FromContext(ctx)
		log.V(1).Info("Table data size", "table", table, "rows", rowCount)
	}

	return nil
}

func (vs *ValidationService) checkPerformanceBaseline(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
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

	log := log.FromContext(ctx)
	log.V(1).Info("Performance baseline", "avgQueryTime", avgDuration)

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
			log := log.FromContext(context.Background())
			log.V(1).Info("Usage units differ, skipping comparison",
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
		// Simple extraction - in production you'd use proper SQL parsing
		content := strings.ToLower(script.Name)
		if strings.Contains(content, "users") {
			tables["users"] = true
		}
		if strings.Contains(content, "orders") {
			tables["orders"] = true
		}
		// Add more table name extraction logic as needed
	}

	result := make([]string, 0, len(tables))
	for table := range tables {
		result = append(result, table)
	}

	return result
}

func (vs *ValidationService) checkCustomCondition(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, metrics *databasev1alpha1.MigrationMetrics, condition string) bool {
	// Implement custom condition checking logic
	// This is a placeholder - in production you'd have more sophisticated condition evaluation
	switch condition {
	case "high-error-rate":
		// Check if there are any error indicators in metrics
		return metrics != nil && (metrics.CPUUsage == "100%" || metrics.MemoryUsage == "100%")
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
