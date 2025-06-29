package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// MetricsCollector implements the MetricsCollector interface
type MetricsCollector struct {
	// Prometheus metrics
	migrationDuration *prometheus.HistogramVec
	migrationStatus   *prometheus.GaugeVec
	migrationProgress *prometheus.GaugeVec
	errorCount        *prometheus.CounterVec
	backupSize        *prometheus.HistogramVec
	rollbackCount     *prometheus.CounterVec

	// Internal state
	metrics map[string]*databasev1alpha1.MigrationMetrics
	mutex   sync.RWMutex
}

// NewMetricsCollector creates a new MetricsCollector instance
func NewMetricsCollector() interfaces.MetricsCollector {
	mc := &MetricsCollector{
		metrics: make(map[string]*databasev1alpha1.MigrationMetrics),
	}

	// Initialize Prometheus metrics
	mc.migrationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "database_migration_duration_seconds",
			Help:    "Duration of database migrations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "name", "database_type", "strategy", "status"},
	)

	mc.migrationStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_migration_status",
			Help: "Current status of database migrations (0=Pending, 1=Running, 2=Succeeded, 3=Failed, 4=RollingBack)",
		},
		[]string{"namespace", "name", "database_type", "strategy"},
	)

	mc.migrationProgress = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "database_migration_progress_percentage",
			Help: "Progress percentage of database migrations (0-100)",
		},
		[]string{"namespace", "name", "database_type", "strategy"},
	)

	mc.errorCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "database_migration_errors_total",
			Help: "Total number of migration errors",
		},
		[]string{"namespace", "name", "database_type", "strategy", "error_type"},
	)

	mc.backupSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "database_migration_backup_size_bytes",
			Help:    "Size of database backups in bytes",
			Buckets: []float64{1024, 10240, 102400, 1048576, 10485760, 104857600, 1073741824}, // 1KB to 1GB
		},
		[]string{"namespace", "name", "database_type"},
	)

	mc.rollbackCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "database_migration_rollbacks_total",
			Help: "Total number of migration rollbacks",
		},
		[]string{"namespace", "name", "database_type", "strategy", "reason"},
	)

	return mc
}

// RecordMigrationStart records the start of a migration
func (mc *MetricsCollector) RecordMigrationStart(migration *databasev1alpha1.DatabaseMigration) {
	logger := log.FromContext(context.Background())

	migrationKey := mc.getMigrationKey(migration)

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Initialize metrics for this migration
	mc.metrics[migrationKey] = &databasev1alpha1.MigrationMetrics{
		Duration:         &metav1.Duration{Duration: 0},
		CPUUsage:         "0%",
		MemoryUsage:      "0%",
		DiskUsage:        "0%",
		QueriesPerSecond: "0",
	}

	// Update status metric
	mc.updateStatusMetric(migration, databasev1alpha1.MigrationPhaseRunning)

	// Update progress metric
	mc.updateProgressMetric(migration, 0)

	logger.V(1).Info("Migration start recorded", "migration", migration.Name)
}

// RecordMigrationEnd records the completion of a migration
func (mc *MetricsCollector) RecordMigrationEnd(migration *databasev1alpha1.DatabaseMigration, success bool) {
	logger := log.FromContext(context.Background())

	migrationKey := mc.getMigrationKey(migration)

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Calculate duration
	var duration time.Duration
	if migration.Status.StartTime != nil {
		if migration.Status.CompletionTime != nil {
			duration = migration.Status.CompletionTime.Sub(migration.Status.StartTime.Time)
		} else {
			duration = time.Since(migration.Status.StartTime.Time)
		}
	}

	// Record duration metric
	status := "failed"
	if success {
		status = "succeeded"
	}

	mc.migrationDuration.WithLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
		status,
	).Observe(duration.Seconds())

	// Update status metric
	if success {
		mc.updateStatusMetric(migration, databasev1alpha1.MigrationPhaseSucceeded)
	} else {
		mc.updateStatusMetric(migration, databasev1alpha1.MigrationPhaseFailed)
	}

	// Update progress metric to 100% for completed migrations
	mc.updateProgressMetric(migration, 100)

	// Clean up metrics
	delete(mc.metrics, migrationKey)

	logger.V(1).Info("Migration end recorded", "migration", migration.Name, "success", success, "duration", duration)
}

// UpdateProgress updates migration progress metrics
func (mc *MetricsCollector) UpdateProgress(migration *databasev1alpha1.DatabaseMigration, progress *databasev1alpha1.ProgressInfo) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Update progress metric
	if progress != nil {
		mc.updateProgressMetric(migration, int(progress.PercentageComplete))
	}

	// Update internal metrics
	migrationKey := mc.getMigrationKey(migration)
	if metrics, exists := mc.metrics[migrationKey]; exists {
		if migration.Status.StartTime != nil {
			metrics.Duration = &metav1.Duration{Duration: time.Since(migration.Status.StartTime.Time)}
		}
	}
}

// RecordError records migration errors
func (mc *MetricsCollector) RecordError(migration *databasev1alpha1.DatabaseMigration, err error) {
	logger := log.FromContext(context.Background())

	// Determine error type
	errorType := "unknown"
	if err != nil {
		errStr := err.Error()
		switch {
		case strings.Contains(errStr, "connection"):
			errorType = "connection"
		case strings.Contains(errStr, "permission"):
			errorType = "permission"
		case strings.Contains(errStr, "timeout"):
			errorType = "timeout"
		case strings.Contains(errStr, "validation"):
			errorType = "validation"
		case strings.Contains(errStr, "rollback"):
			errorType = "rollback"
		}
	}

	// Increment error counter
	mc.errorCount.WithLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
		errorType,
	).Inc()

	logger.V(1).Info("Migration error recorded", "migration", migration.Name, "error_type", errorType)
}

// GetMetrics returns current metrics for a migration
func (mc *MetricsCollector) GetMetrics(migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.MigrationMetrics, error) {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	migrationKey := mc.getMigrationKey(migration)

	if metrics, exists := mc.metrics[migrationKey]; exists {
		// Update duration if migration is still running
		if migration.Status.StartTime != nil && migration.Status.Phase == databasev1alpha1.MigrationPhaseRunning {
			metrics.Duration = &metav1.Duration{Duration: time.Since(migration.Status.StartTime.Time)}
		}

		return metrics, nil
	}

	return nil, fmt.Errorf("no metrics found for migration %s", migration.Name)
}

// RecordBackupSize records backup size metrics
func (mc *MetricsCollector) RecordBackupSize(migration *databasev1alpha1.DatabaseMigration, sizeBytes int64) {
	mc.backupSize.WithLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
	).Observe(float64(sizeBytes))
}

// RecordRollback records rollback metrics
func (mc *MetricsCollector) RecordRollback(migration *databasev1alpha1.DatabaseMigration, reason string) {
	mc.rollbackCount.WithLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
		reason,
	).Inc()
}

// Helper methods

func (mc *MetricsCollector) getMigrationKey(migration *databasev1alpha1.DatabaseMigration) string {
	return fmt.Sprintf("%s/%s", migration.Namespace, migration.Name)
}

func (mc *MetricsCollector) updateStatusMetric(migration *databasev1alpha1.DatabaseMigration, phase databasev1alpha1.MigrationPhase) {
	// Convert phase to numeric value
	var statusValue float64
	switch phase {
	case databasev1alpha1.MigrationPhasePending:
		statusValue = 0
	case databasev1alpha1.MigrationPhaseRunning:
		statusValue = 1
	case databasev1alpha1.MigrationPhaseSucceeded:
		statusValue = 2
	case databasev1alpha1.MigrationPhaseFailed:
		statusValue = 3
	case databasev1alpha1.MigrationPhaseRollingBack:
		statusValue = 4
	default:
		statusValue = 0
	}

	// Remove old metric first
	mc.migrationStatus.DeleteLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
	)

	// Set new metric
	mc.migrationStatus.WithLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
	).Set(statusValue)
}

func (mc *MetricsCollector) updateProgressMetric(migration *databasev1alpha1.DatabaseMigration, percentage int) {
	// Remove old metric first
	mc.migrationProgress.DeleteLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
	)

	// Set new metric
	mc.migrationProgress.WithLabelValues(
		migration.Namespace,
		migration.Name,
		string(migration.Spec.Database.Type),
		string(migration.Spec.Migration.Strategy),
	).Set(float64(percentage))
}
