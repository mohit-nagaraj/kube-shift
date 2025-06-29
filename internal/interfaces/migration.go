package interfaces

import (
	"context"
	"database/sql"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
)

// MigrationEngine defines the interface for database-specific migration operations
type MigrationEngine interface {
	// ValidateConnection checks if we can connect to the database
	ValidateConnection(ctx context.Context, config databasev1alpha1.DatabaseConfig, credentials map[string]string) error

	// GetConnection returns a database connection
	GetConnection(ctx context.Context, config databasev1alpha1.DatabaseConfig, credentials map[string]string) (*sql.DB, error)

	// CreateShadowTable creates a shadow table for migration
	CreateShadowTable(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error

	// SyncData synchronizes data from original to shadow table
	SyncData(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error

	// SwapTables atomically swaps original and shadow tables
	SwapTables(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error

	// Rollback performs rollback operations
	Rollback(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error

	// GetMetrics returns current migration metrics
	GetMetrics(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.MigrationMetrics, error)

	// ValidateDataIntegrity checks data integrity between tables
	ValidateDataIntegrity(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error

	// CreateBackup creates a backup before migration
	CreateBackup(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.BackupInfo, error)

	// RestoreFromBackup restores database from backup
	RestoreFromBackup(ctx context.Context, db *sql.DB, backupInfo *databasev1alpha1.BackupInfo) error
}

// MetricsCollector defines the interface for collecting migration metrics
type MetricsCollector interface {
	// RecordMigrationStart records the start of a migration
	RecordMigrationStart(migration *databasev1alpha1.DatabaseMigration)

	// RecordMigrationEnd records the completion of a migration
	RecordMigrationEnd(migration *databasev1alpha1.DatabaseMigration, success bool)

	// UpdateProgress updates migration progress metrics
	UpdateProgress(migration *databasev1alpha1.DatabaseMigration, progress *databasev1alpha1.ProgressInfo)

	// RecordError records migration errors
	RecordError(migration *databasev1alpha1.DatabaseMigration, err error)

	// GetMetrics returns current metrics for a migration
	GetMetrics(migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.MigrationMetrics, error)
}

// NotificationService defines the interface for sending notifications
type NotificationService interface {
	// SendMigrationStarted sends notification when migration starts
	SendMigrationStarted(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error

	// SendMigrationCompleted sends notification when migration completes
	SendMigrationCompleted(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error

	// SendMigrationFailed sends notification when migration fails
	SendMigrationFailed(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, err error) error

	// SendRollbackStarted sends notification when rollback starts
	SendRollbackStarted(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error
}

// BackupService defines the interface for backup operations
type BackupService interface {
	// CreateBackup creates a backup of the database
	CreateBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.BackupInfo, error)

	// RestoreBackup restores a database from backup
	RestoreBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupInfo *databasev1alpha1.BackupInfo) error

	// DeleteBackup removes a backup
	DeleteBackup(ctx context.Context, backupInfo *databasev1alpha1.BackupInfo, migration *databasev1alpha1.DatabaseMigration) error

	// ValidateBackup checks if a backup is valid
	ValidateBackup(ctx context.Context, backupInfo *databasev1alpha1.BackupInfo, migration *databasev1alpha1.DatabaseMigration) error
}

// ScriptLoader defines the interface for loading migration scripts
type ScriptLoader interface {
	// LoadScript loads a migration script from various sources
	LoadScript(ctx context.Context, script databasev1alpha1.MigrationScript) (string, error)

	// ValidateScript validates script syntax and checksum
	ValidateScript(ctx context.Context, script databasev1alpha1.MigrationScript, content string) error
}

// ValidationService defines the interface for migration validation
type ValidationService interface {
	// RunPreChecks runs pre-migration validation checks
	RunPreChecks(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error

	// ValidatePerformance checks if migration meets performance thresholds
	ValidatePerformance(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, metrics *databasev1alpha1.MigrationMetrics) error

	// ShouldRollback determines if migration should be rolled back
	ShouldRollback(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, metrics *databasev1alpha1.MigrationMetrics) (bool, string, error)
}
