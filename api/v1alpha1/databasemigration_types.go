/*
Copyright 2025.

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
// api/v1alpha1/databasemigration_types.go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DatabaseType defines the supported database types
// +kubebuilder:validation:Enum=postgresql;mysql;mariadb
type DatabaseType string

const (
	DatabaseTypePostgreSQL DatabaseType = "postgresql"
	DatabaseTypeMySQL      DatabaseType = "mysql"
	DatabaseTypeMariaDB    DatabaseType = "mariadb"
)

// MigrationStrategy defines the migration approach
// +kubebuilder:validation:Enum=shadow-table;blue-green;rolling
type MigrationStrategy string

const (
	MigrationStrategyShadowTable MigrationStrategy = "shadow-table"
	MigrationStrategyBlueGreen   MigrationStrategy = "blue-green"
	MigrationStrategyRolling     MigrationStrategy = "rolling"
)

// MigrationPhase represents the current phase of migration
type MigrationPhase string

const (
	MigrationPhasePending     MigrationPhase = "Pending"
	MigrationPhaseRunning     MigrationPhase = "Running"
	MigrationPhaseSucceeded   MigrationPhase = "Succeeded"
	MigrationPhaseFailed      MigrationPhase = "Failed"
	MigrationPhaseRollingBack MigrationPhase = "RollingBack"
)

// ScriptType defines the types of migration scripts
// +kubebuilder:validation:Enum=schema;data;validation
type ScriptType string

const (
	ScriptTypeSchema     ScriptType = "schema"
	ScriptTypeData       ScriptType = "data"
	ScriptTypeValidation ScriptType = "validation"
)

// DatabaseConfig holds database connection information
type DatabaseConfig struct {
	// Type of database (postgresql, mysql, mariadb)
	// +kubebuilder:validation:Required
	Type DatabaseType `json:"type"`

	// ConnectionSecret is the name of the secret containing database credentials
	// +kubebuilder:validation:Required
	ConnectionSecret string `json:"connectionSecret"`

	// Host is the database host
	// +kubebuilder:validation:Required
	Host string `json:"host"`

	// Port is the database port
	// +kubebuilder:default=5432
	Port int32 `json:"port,omitempty"`

	// Database name
	// +kubebuilder:validation:Required
	Database string `json:"database"`

	// SSL mode for connection
	// +kubebuilder:default="require"
	SSLMode string `json:"sslMode,omitempty"`
}

// MigrationScript represents a single migration script
type MigrationScript struct {
	// Name of the script
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Type of script (schema, data, validation)
	// +kubebuilder:validation:Required
	Type ScriptType `json:"type"`

	// Source of the script (configmap://, secret://, inline)
	// +kubebuilder:validation:Required
	Source string `json:"source"`

	// Checksum for verification
	Checksum string `json:"checksum,omitempty"`

	// DependsOn specifies scripts that must complete before this one
	DependsOn []string `json:"dependsOn,omitempty"`

	// Timeout for script execution
	// +kubebuilder:default="10m"
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

// PreCheck defines validation checks before migration
type PreCheck struct {
	// Type of check (connection, permissions, disk-space)
	// +kubebuilder:validation:Required
	Type string `json:"type"`

	// Threshold for checks that need it (e.g., disk-space)
	Threshold string `json:"threshold,omitempty"`
}

// ValidationConfig holds validation settings
type ValidationConfig struct {
	// PreChecks to run before migration
	PreChecks []PreCheck `json:"preChecks,omitempty"`

	// DataIntegrityChecks configuration
	DataIntegrityChecks DataIntegrityChecks `json:"dataIntegrityChecks,omitempty"`

	// PerformanceThresholds for monitoring
	PerformanceThresholds PerformanceThresholds `json:"performanceThresholds,omitempty"`
}

// DataIntegrityChecks configuration
type DataIntegrityChecks struct {
	// Enabled turns on/off data integrity checks
	// +kubebuilder:default=true
	Enabled bool `json:"enabled"`

	// SamplePercentage of data to check (1-100)
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	SamplePercentage int32 `json:"samplePercentage,omitempty"`

	// ChecksumValidation enables checksum-based validation
	// +kubebuilder:default=true
	ChecksumValidation bool `json:"checksumValidation"`
}

// PerformanceThresholds defines performance limits
type PerformanceThresholds struct {
	// MaxMigrationTime is the maximum allowed migration duration
	// +kubebuilder:default="30m"
	MaxMigrationTime *metav1.Duration `json:"maxMigrationTime,omitempty"`

	// MaxDowntime is the maximum allowed downtime
	// +kubebuilder:default="0s"
	MaxDowntime *metav1.Duration `json:"maxDowntime,omitempty"`

	// MaxCPUUsage is the maximum CPU usage percentage
	// +kubebuilder:default="80%"
	MaxCPUUsage string `json:"maxCpuUsage,omitempty"`

	// MaxMemoryUsage is the maximum memory usage percentage
	// +kubebuilder:default="70%"
	MaxMemoryUsage string `json:"maxMemoryUsage,omitempty"`
}

// RollbackStrategy defines rollback approach
// +kubebuilder:validation:Enum=immediate;graceful
type RollbackStrategy string

const (
	RollbackStrategyImmediate RollbackStrategy = "immediate"
	RollbackStrategyGraceful  RollbackStrategy = "graceful"
)

// RollbackConfig holds rollback settings
type RollbackConfig struct {
	// Automatic enables automatic rollback
	// +kubebuilder:default=true
	Automatic bool `json:"automatic"`

	// Conditions that trigger automatic rollback
	Conditions []string `json:"conditions,omitempty"`

	// Strategy for rollback execution
	// +kubebuilder:default="immediate"
	Strategy RollbackStrategy `json:"strategy,omitempty"`

	// PreserveBackup keeps backup after successful migration
	// +kubebuilder:default=true
	PreserveBackup bool `json:"preserveBackup"`
}

// NotificationConfig holds notification settings
type NotificationConfig struct {
	// Slack webhook configuration
	Slack *SlackConfig `json:"slack,omitempty"`

	// Email notification configuration
	Email *EmailConfig `json:"email,omitempty"`
}

// SlackConfig holds Slack notification settings
type SlackConfig struct {
	// Webhook secret name containing Slack webhook URL
	Webhook string `json:"webhook"`
}

// EmailConfig holds email notification settings
type EmailConfig struct {
	// Recipients list
	Recipients []string `json:"recipients"`
}

// MigrationConfig holds migration-specific settings
type MigrationConfig struct {
	// Strategy for migration execution
	// +kubebuilder:default="shadow-table"
	Strategy MigrationStrategy `json:"strategy,omitempty"`

	// Scripts to execute during migration
	// +kubebuilder:validation:MinItems=1
	Scripts []MigrationScript `json:"scripts"`

	// Parallel execution settings
	Parallel bool `json:"parallel,omitempty"`

	// BatchSize for data migration
	// +kubebuilder:default=1000
	BatchSize int32 `json:"batchSize,omitempty"`
}

// DatabaseMigrationSpec defines the desired state of DatabaseMigration
type DatabaseMigrationSpec struct {
	// Database connection configuration
	// +kubebuilder:validation:Required
	Database DatabaseConfig `json:"database"`

	// Migration configuration
	// +kubebuilder:validation:Required
	Migration MigrationConfig `json:"migration"`

	// Validation configuration
	Validation *ValidationConfig `json:"validation,omitempty"`

	// Rollback configuration
	Rollback *RollbackConfig `json:"rollback,omitempty"`

	// Notification configuration
	Notifications *NotificationConfig `json:"notifications,omitempty"`
}

// ProgressInfo tracks migration progress
type ProgressInfo struct {
	// TotalSteps in the migration
	TotalSteps int32 `json:"totalSteps"`

	// CompletedSteps so far
	CompletedSteps int32 `json:"completedSteps"`

	// CurrentStep being executed
	CurrentStep string `json:"currentStep,omitempty"`

	// Percentage complete (0-100)
	PercentageComplete int32 `json:"percentageComplete"`
}

// ShadowTableInfo tracks shadow table status
type ShadowTableInfo struct {
	// Name of the shadow table
	Name string `json:"name,omitempty"`

	// SyncStatus of the shadow table
	SyncStatus string `json:"syncStatus,omitempty"`

	// RowsProcessed in synchronization
	RowsProcessed int64 `json:"rowsProcessed"`

	// TotalRows to process
	TotalRows int64 `json:"totalRows"`

	// LastSyncTime when sync was last updated
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`
}

// MigrationMetrics holds performance metrics
type MigrationMetrics struct {
	// Duration of migration so far
	Duration *metav1.Duration `json:"duration,omitempty"`

	// CPU usage percentage
	CPUUsage string `json:"cpuUsage,omitempty"`

	// Memory usage percentage
	MemoryUsage string `json:"memoryUsage,omitempty"`

	// Disk usage
	DiskUsage string `json:"diskUsage,omitempty"`

	// QueriesPerSecond during migration
	QueriesPerSecond string `json:"queriesPerSecond,omitempty"`
}

// BackupInfo holds backup information
type BackupInfo struct {
	// Location of the backup
	Location string `json:"location,omitempty"`

	// Size of the backup
	Size string `json:"size,omitempty"`

	// CreatedAt timestamp
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`
}

// BlueGreenInfo tracks blue-green migration status
type BlueGreenInfo struct {
	// Phase of blue-green migration
	Phase string `json:"phase,omitempty"`

	// GreenDatabase name
	GreenDatabase string `json:"greenDatabase,omitempty"`

	// BlueDatabase name (original)
	BlueDatabase string `json:"blueDatabase,omitempty"`

	// TrafficSwitched indicates if traffic has been switched to green
	TrafficSwitched bool `json:"trafficSwitched,omitempty"`

	// ValidationPassed indicates if green database validation passed
	ValidationPassed bool `json:"validationPassed,omitempty"`
}

// RollingInfo tracks rolling migration status
type RollingInfo struct {
	// Phase of rolling migration
	Phase string `json:"phase,omitempty"`

	// TotalSteps in the rolling migration
	TotalSteps int32 `json:"totalSteps"`

	// CurrentStep being executed
	CurrentStep int32 `json:"currentStep"`

	// FailedSteps tracks failed steps for rollback
	FailedSteps []int32 `json:"failedSteps,omitempty"`

	// RollbackPoint for failed migrations
	RollbackPoint int32 `json:"rollbackPoint,omitempty"`
}

// DatabaseMigrationStatus defines the observed state of DatabaseMigration
type DatabaseMigrationStatus struct {
	// Phase of the migration
	Phase MigrationPhase `json:"phase,omitempty"`

	// StartTime when migration began
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime when migration finished
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Progress information
	Progress *ProgressInfo `json:"progress,omitempty"`

	// ShadowTable information (for shadow-table strategy)
	ShadowTable *ShadowTableInfo `json:"shadowTable,omitempty"`

	// Metrics collected during migration
	Metrics *MigrationMetrics `json:"metrics,omitempty"`

	// BackupInfo for rollback purposes
	BackupInfo *BackupInfo `json:"backupInfo,omitempty"`

	// BlueGreen information (for blue-green strategy)
	BlueGreen *BlueGreenInfo `json:"blueGreen,omitempty"`

	// Rolling information (for rolling strategy)
	Rolling *RollingInfo `json:"rolling,omitempty"`

	// Message with human-readable status
	Message string `json:"message,omitempty"`

	// Reason for current status
	Reason string `json:"reason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Strategy",type="string",JSONPath=".spec.migration.strategy"
// +kubebuilder:printcolumn:name="Database",type="string",JSONPath=".spec.database.type"
// +kubebuilder:printcolumn:name="Progress",type="string",JSONPath=".status.progress.percentageComplete"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// DatabaseMigration is the Schema for the databasemigrations API
type DatabaseMigration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DatabaseMigrationSpec   `json:"spec,omitempty"`
	Status DatabaseMigrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DatabaseMigrationList contains a list of DatabaseMigration
type DatabaseMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatabaseMigration `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseMigration{}, &DatabaseMigrationList{})
}
