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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// DatabaseMigrationReconciler reconciles a DatabaseMigration object
type DatabaseMigrationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger

	// Migration engines for different database types
	PostgreSQLEngine interfaces.MigrationEngine
	MySQLEngine      interfaces.MigrationEngine
	MariaDBEngine    interfaces.MigrationEngine

	// Services
	MetricsCollector    interfaces.MetricsCollector
	NotificationService interfaces.NotificationService
	BackupService       interfaces.BackupService
	ScriptLoader        interfaces.ScriptLoader
	ValidationService   interfaces.ValidationService
}

// Finalizer name for cleanup
const DatabaseMigrationFinalizer = "database.mohitnagaraj.in/finalizer"

//+kubebuilder:rbac:groups=database.mohitnagaraj.in,resources=databasemigrations,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=database.mohitnagaraj.in,resources=databasemigrations/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=database.mohitnagaraj.in,resources=databasemigrations/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop
func (r *DatabaseMigrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx).WithValues("databasemigration", req.NamespacedName)

	// Fetch the DatabaseMigration instance
	migration := &databasev1alpha1.DatabaseMigration{}
	if err := r.Get(ctx, req.NamespacedName, migration); err != nil {
		if apierrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			log.Info("DatabaseMigration resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get DatabaseMigration")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if migration.DeletionTimestamp != nil {
		return r.handleDeletion(ctx, migration)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(migration, DatabaseMigrationFinalizer) {
		controllerutil.AddFinalizer(migration, DatabaseMigrationFinalizer)
		if err := r.Update(ctx, migration); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle the migration based on current phase
	return r.handleMigrationPhase(ctx, migration)
}

// handleMigrationPhase handles migration based on current phase
func (r *DatabaseMigrationReconciler) handleMigrationPhase(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	switch migration.Status.Phase {
	case "":
		// Initialize migration
		return r.initializeMigration(ctx, migration)
	case databasev1alpha1.MigrationPhasePending:
		// Run pre-checks and start migration
		return r.startMigration(ctx, migration)
	case databasev1alpha1.MigrationPhaseRunning:
		// Continue migration execution
		return r.continueMigration(ctx, migration)
	case databasev1alpha1.MigrationPhaseRollingBack:
		// Handle rollback
		return r.handleRollback(ctx, migration)
	case databasev1alpha1.MigrationPhaseSucceeded, databasev1alpha1.MigrationPhaseFailed:
		// Migration completed, just monitor
		log.Info("Migration completed", "phase", migration.Status.Phase)
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown migration phase", "phase", migration.Status.Phase)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
}

// initializeMigration sets up the migration
func (r *DatabaseMigrationReconciler) initializeMigration(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Initializing migration")

	// Update status to pending
	migration.Status.Phase = databasev1alpha1.MigrationPhasePending
	migration.Status.StartTime = &metav1.Time{Time: time.Now()}
	migration.Status.Message = "Initializing migration"
	migration.Status.Reason = "Initialization"

	// Initialize progress
	migration.Status.Progress = &databasev1alpha1.ProgressInfo{
		TotalSteps:         int32(len(migration.Spec.Migration.Scripts) + 3), // scripts + validation + backup + swap
		CompletedSteps:     0,
		CurrentStep:        "initialization",
		PercentageComplete: 0,
	}

	// Set initial condition
	r.setCondition(migration, "Initialized", metav1.ConditionTrue, "InitializationComplete", "Migration initialized successfully")

	if err := r.Status().Update(ctx, migration); err != nil {
		log.Error(err, "Failed to update migration status during initialization")
		return ctrl.Result{}, err
	}

	// Record metrics
	if r.MetricsCollector != nil {
		r.MetricsCollector.RecordMigrationStart(migration)
	}

	// Send notification
	if r.NotificationService != nil && migration.Spec.Notifications != nil {
		if err := r.NotificationService.SendMigrationStarted(ctx, migration); err != nil {
			log.Error(err, "Failed to send migration started notification")
			// Don't fail the migration for notification errors
		}
	}

	return ctrl.Result{Requeue: true}, nil
}

// startMigration begins the migration process
func (r *DatabaseMigrationReconciler) startMigration(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Starting migration")

	// Run pre-checks
	if r.ValidationService != nil {
		if err := r.ValidationService.RunPreChecks(ctx, migration); err != nil {
			return r.handleMigrationError(ctx, migration, fmt.Errorf("pre-check validation failed: %w", err))
		}
	}

	// Create backup if needed
	if migration.Spec.Rollback != nil && migration.Spec.Rollback.PreserveBackup {
		backupInfo, err := r.createBackup(ctx, migration)
		if err != nil {
			return r.handleMigrationError(ctx, migration, fmt.Errorf("backup creation failed: %w", err))
		}
		migration.Status.BackupInfo = backupInfo
	}

	// Update status to running
	migration.Status.Phase = databasev1alpha1.MigrationPhaseRunning
	migration.Status.Message = "Migration in progress"
	migration.Status.Reason = "MigrationStarted"
	migration.Status.Progress.CurrentStep = "running-migration"
	migration.Status.Progress.CompletedSteps = 1

	r.setCondition(migration, "Running", metav1.ConditionTrue, "MigrationStarted", "Migration execution started")

	if err := r.Status().Update(ctx, migration); err != nil {
		log.Error(err, "Failed to update migration status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 30}, nil
}

// continueMigration continues the migration execution
func (r *DatabaseMigrationReconciler) continueMigration(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Continuing migration")

	// Get the appropriate migration engine
	engine, err := r.getMigrationEngine(migration.Spec.Database.Type)
	if err != nil {
		return r.handleMigrationError(ctx, migration, err)
	}

	// Execute migration based on strategy
	switch migration.Spec.Migration.Strategy {
	case databasev1alpha1.MigrationStrategyShadowTable:
		return r.executeShadowTableMigration(ctx, migration, engine)
	case databasev1alpha1.MigrationStrategyBlueGreen:
		return r.executeBlueGreenMigration(ctx, migration, engine)
	case databasev1alpha1.MigrationStrategyRolling:
		return r.executeRollingMigration(ctx, migration, engine)
	default:
		return r.handleMigrationError(ctx, migration, fmt.Errorf("unsupported migration strategy: %s", migration.Spec.Migration.Strategy))
	}
}

// handleRollback handles migration rollback
func (r *DatabaseMigrationReconciler) handleRollback(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Handling migration rollback")

	engine, err := r.getMigrationEngine(migration.Spec.Database.Type)
	if err != nil {
		return r.handleMigrationError(ctx, migration, err)
	}

	// Get database connection
	db, err := r.getDatabaseConnection(ctx, migration)
	if err != nil {
		return r.handleMigrationError(ctx, migration, fmt.Errorf("failed to get database connection for rollback: %w", err))
	}
	defer db.Close()

	// Perform rollback
	if err := engine.Rollback(ctx, db, migration); err != nil {
		migration.Status.Phase = databasev1alpha1.MigrationPhaseFailed
		migration.Status.Message = fmt.Sprintf("Rollback failed: %v", err)
		migration.Status.Reason = "RollbackFailed"
		r.setCondition(migration, "Failed", metav1.ConditionTrue, "RollbackFailed", fmt.Sprintf("Rollback failed: %v", err))
	} else {
		migration.Status.Phase = databasev1alpha1.MigrationPhaseFailed
		migration.Status.Message = "Migration rolled back successfully"
		migration.Status.Reason = "RollbackCompleted"
		r.setCondition(migration, "RolledBack", metav1.ConditionTrue, "RollbackCompleted", "Migration rolled back successfully")
	}

	migration.Status.CompletionTime = &metav1.Time{Time: time.Now()}

	if err := r.Status().Update(ctx, migration); err != nil {
		log.Error(err, "Failed to update migration status after rollback")
		return ctrl.Result{}, err
	}

	// Send notification
	if r.NotificationService != nil && migration.Spec.Notifications != nil {
		if err := r.NotificationService.SendRollbackStarted(ctx, migration); err != nil {
			log.Error(err, "Failed to send rollback notification")
		}
	}

	return ctrl.Result{}, nil
}

// handleDeletion handles migration resource deletion
func (r *DatabaseMigrationReconciler) handleDeletion(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Handling migration deletion")

	// Perform cleanup if migration is in progress
	if migration.Status.Phase == databasev1alpha1.MigrationPhaseRunning {
		log.Info("Migration is running, performing cleanup before deletion")

		engine, err := r.getMigrationEngine(migration.Spec.Database.Type)
		if err != nil {
			log.Error(err, "Failed to get migration engine for cleanup")
		} else {
			db, err := r.getDatabaseConnection(ctx, migration)
			if err != nil {
				log.Error(err, "Failed to get database connection for cleanup")
			} else {
				defer db.Close()
				if err := engine.Rollback(ctx, db, migration); err != nil {
					log.Error(err, "Failed to rollback during cleanup")
				}
			}
		}
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(migration, DatabaseMigrationFinalizer)
	if err := r.Update(ctx, migration); err != nil {
		log.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getMigrationEngine returns the appropriate migration engine for the database type
func (r *DatabaseMigrationReconciler) getMigrationEngine(dbType databasev1alpha1.DatabaseType) (interfaces.MigrationEngine, error) {
	switch dbType {
	case databasev1alpha1.DatabaseTypePostgreSQL:
		if r.PostgreSQLEngine == nil {
			return nil, fmt.Errorf("PostgreSQL migration engine not configured")
		}
		return r.PostgreSQLEngine, nil
	case databasev1alpha1.DatabaseTypeMySQL:
		if r.MySQLEngine == nil {
			return nil, fmt.Errorf("MySQL migration engine not configured")
		}
		return r.MySQLEngine, nil
	case databasev1alpha1.DatabaseTypeMariaDB:
		if r.MariaDBEngine == nil {
			return nil, fmt.Errorf("MariaDB migration engine not configured")
		}
		return r.MariaDBEngine, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// getDatabaseConnection establishes a database connection
func (r *DatabaseMigrationReconciler) getDatabaseConnection(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*sql.DB, error) {
	// Get database credentials from secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := r.Get(ctx, secretKey, secret); err != nil {
		return nil, fmt.Errorf("failed to get database secret: %w", err)
	}

	// Convert secret data to map
	credentials := make(map[string]string)
	for key, value := range secret.Data {
		credentials[key] = string(value)
	}

	// Get migration engine
	engine, err := r.getMigrationEngine(migration.Spec.Database.Type)
	if err != nil {
		return nil, err
	}

	// Get connection
	return engine.GetConnection(ctx, migration.Spec.Database, credentials)
}

// executeShadowTableMigration executes shadow table migration strategy
func (r *DatabaseMigrationReconciler) executeShadowTableMigration(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, engine interfaces.MigrationEngine) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Executing shadow table migration")

	db, err := r.getDatabaseConnection(ctx, migration)
	if err != nil {
		return r.handleMigrationError(ctx, migration, fmt.Errorf("failed to get database connection: %w", err))
	}
	defer db.Close()

	// Initialize shadow table info if not present
	if migration.Status.ShadowTable == nil {
		migration.Status.ShadowTable = &databasev1alpha1.ShadowTableInfo{
			Name:       fmt.Sprintf("%s_shadow_%d", "main_table", time.Now().Unix()),
			SyncStatus: "Creating",
		}
	}

	switch migration.Status.ShadowTable.SyncStatus {
	case "Creating", "":
		// Step 1: Create shadow table
		if err := engine.CreateShadowTable(ctx, db, migration); err != nil {
			return r.handleMigrationError(ctx, migration, fmt.Errorf("failed to create shadow table: %w", err))
		}

		migration.Status.ShadowTable.SyncStatus = "Syncing"
		migration.Status.Progress.CurrentStep = "syncing-data"
		migration.Status.Progress.CompletedSteps = 2

		if err := r.Status().Update(ctx, migration); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: time.Second * 10}, nil

	case "Syncing":
		// Step 2: Sync data
		if err := engine.SyncData(ctx, db, migration); err != nil {
			return r.handleMigrationError(ctx, migration, fmt.Errorf("failed to sync data: %w", err))
		}

		// Get current metrics to check progress
		metrics, err := engine.GetMetrics(ctx, db, migration)
		if err != nil {
			log.Error(err, "Failed to get migration metrics")
		} else {
			migration.Status.Metrics = metrics
		}

		// Check if sync is complete (this would be determined by the engine)
		if migration.Status.ShadowTable.RowsProcessed >= migration.Status.ShadowTable.TotalRows {
			migration.Status.ShadowTable.SyncStatus = "InSync"
			migration.Status.Progress.CurrentStep = "swapping-tables"
			migration.Status.Progress.CompletedSteps = 3
		}

		if err := r.Status().Update(ctx, migration); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: time.Second * 30}, nil

	case "InSync":
		// Step 3: Atomic swap
		if err := engine.SwapTables(ctx, db, migration); err != nil {
			return r.handleMigrationError(ctx, migration, fmt.Errorf("failed to swap tables: %w", err))
		}

		// Step 4: Validate data integrity
		if err := engine.ValidateDataIntegrity(ctx, db, migration); err != nil {
			// Start rollback
			migration.Status.Phase = databasev1alpha1.MigrationPhaseRollingBack
			migration.Status.Message = fmt.Sprintf("Data integrity validation failed: %v", err)
			migration.Status.Reason = "DataIntegrityFailed"

			if err := r.Status().Update(ctx, migration); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{RequeueAfter: time.Second * 5}, nil
		}

		// Migration successful
		migration.Status.Phase = databasev1alpha1.MigrationPhaseSucceeded
		migration.Status.Message = "Migration completed successfully"
		migration.Status.Reason = "MigrationCompleted"
		migration.Status.CompletionTime = &metav1.Time{Time: time.Now()}
		migration.Status.Progress.CompletedSteps = migration.Status.Progress.TotalSteps
		migration.Status.Progress.PercentageComplete = 100
		migration.Status.Progress.CurrentStep = "completed"

		r.setCondition(migration, "Succeeded", metav1.ConditionTrue, "MigrationCompleted", "Migration completed successfully")

		if err := r.Status().Update(ctx, migration); err != nil {
			return ctrl.Result{}, err
		}

		// Send completion notification
		if r.NotificationService != nil && migration.Spec.Notifications != nil {
			if err := r.NotificationService.SendMigrationCompleted(ctx, migration); err != nil {
				log.Error(err, "Failed to send migration completion notification")
			}
		}

		// Record metrics
		if r.MetricsCollector != nil {
			r.MetricsCollector.RecordMigrationEnd(migration, true)
		}

		return ctrl.Result{}, nil
	}

	return ctrl.Result{RequeueAfter: time.Minute}, nil
}

// executeBlueGreenMigration executes blue-green migration strategy
func (r *DatabaseMigrationReconciler) executeBlueGreenMigration(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, engine interfaces.MigrationEngine) (ctrl.Result, error) {
	// TODO: Implement blue-green migration strategy
	return r.handleMigrationError(ctx, migration, fmt.Errorf("blue-green migration strategy not yet implemented"))
}

// executeRollingMigration executes rolling migration strategy
func (r *DatabaseMigrationReconciler) executeRollingMigration(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, engine interfaces.MigrationEngine) (ctrl.Result, error) {
	// TODO: Implement rolling migration strategy
	return r.handleMigrationError(ctx, migration, fmt.Errorf("rolling migration strategy not yet implemented"))
}

// createBackup creates a backup before migration
func (r *DatabaseMigrationReconciler) createBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.BackupInfo, error) {
	if r.BackupService == nil {
		return nil, fmt.Errorf("backup service not configured")
	}

	return r.BackupService.CreateBackup(ctx, migration)
}

// handleMigrationError handles migration errors and updates status
func (r *DatabaseMigrationReconciler) handleMigrationError(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, err error) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Error(err, "Migration error occurred")

	// Check if automatic rollback is enabled
	if migration.Spec.Rollback != nil && migration.Spec.Rollback.Automatic {
		migration.Status.Phase = databasev1alpha1.MigrationPhaseRollingBack
		migration.Status.Message = fmt.Sprintf("Starting automatic rollback due to error: %v", err)
		migration.Status.Reason = "AutomaticRollback"

		r.setCondition(migration, "RollingBack", metav1.ConditionTrue, "AutomaticRollback", migration.Status.Message)

		if updateErr := r.Status().Update(ctx, migration); updateErr != nil {
			log.Error(updateErr, "Failed to update status for rollback")
			return ctrl.Result{}, updateErr
		}

		return ctrl.Result{RequeueAfter: time.Second * 5}, nil
	}

	// No automatic rollback, mark as failed
	migration.Status.Phase = databasev1alpha1.MigrationPhaseFailed
	migration.Status.Message = fmt.Sprintf("Migration failed: %v", err)
	migration.Status.Reason = "MigrationFailed"
	migration.Status.CompletionTime = &metav1.Time{Time: time.Now()}

	r.setCondition(migration, "Failed", metav1.ConditionTrue, "MigrationFailed", migration.Status.Message)

	if updateErr := r.Status().Update(ctx, migration); updateErr != nil {
		log.Error(updateErr, "Failed to update migration status")
		return ctrl.Result{}, updateErr
	}

	// Send failure notification
	if r.NotificationService != nil && migration.Spec.Notifications != nil {
		if notifyErr := r.NotificationService.SendMigrationFailed(ctx, migration, err); notifyErr != nil {
			log.Error(notifyErr, "Failed to send migration failure notification")
		}
	}

	// Record error metrics
	if r.MetricsCollector != nil {
		r.MetricsCollector.RecordError(migration, err)
		r.MetricsCollector.RecordMigrationEnd(migration, false)
	}

	return ctrl.Result{}, nil
}

// setCondition sets a condition on the migration status
func (r *DatabaseMigrationReconciler) setCondition(migration *databasev1alpha1.DatabaseMigration, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	// Find existing condition and update it
	for i, existingCondition := range migration.Status.Conditions {
		if existingCondition.Type == conditionType {
			if existingCondition.Status != status ||
				existingCondition.Reason != reason ||
				existingCondition.Message != message {
				migration.Status.Conditions[i] = condition
			}
			return
		}
	}

	// Add new condition
	migration.Status.Conditions = append(migration.Status.Conditions, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseMigrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1alpha1.DatabaseMigration{}).
		Named("databasemigration").
		Complete(r)
}
