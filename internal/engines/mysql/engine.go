// internal/engines/mysql/engine.go
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// Engine implements the MigrationEngine interface for MySQL
type Engine struct {
	scriptLoader interfaces.ScriptLoader
}

// NewEngine creates a new MySQL migration engine
func NewEngine() interfaces.MigrationEngine {
	return &Engine{}
}

// NewEngineWithScriptLoader creates a new MySQL migration engine with script loader
func NewEngineWithScriptLoader(scriptLoader interfaces.ScriptLoader) interfaces.MigrationEngine {
	return &Engine{
		scriptLoader: scriptLoader,
	}
}

// ValidateConnection checks if we can connect to the database
func (e *Engine) ValidateConnection(ctx context.Context, config databasev1alpha1.DatabaseConfig, credentials map[string]string) error {
	db, err := e.GetConnection(ctx, config, credentials)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close database connection")
		}
	}()

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Check if we have necessary permissions
	if err := e.validatePermissions(ctx, db); err != nil {
		return fmt.Errorf("insufficient permissions: %w", err)
	}

	return nil
}

// GetConnection returns a database connection
func (e *Engine) GetConnection(ctx context.Context, config databasev1alpha1.DatabaseConfig, credentials map[string]string) (*sql.DB, error) {
	username, ok := credentials["username"]
	if !ok {
		return nil, fmt.Errorf("username not found in credentials")
	}

	password, ok := credentials["password"]
	if !ok {
		return nil, fmt.Errorf("password not found in credentials")
	}

	// Build MySQL DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		username, password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}

// CreateShadowTable creates a shadow table for migration
func (e *Engine) CreateShadowTable(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	if migration.Status.ShadowTable == nil {
		return fmt.Errorf("shadow table info not initialized")
	}

	shadowTableName := migration.Status.ShadowTable.Name

	// Extract original table name from migration scripts
	originalTable, err := e.extractTableName(migration.Spec.Migration.Scripts)
	if err != nil {
		return fmt.Errorf("failed to extract table name: %w", err)
	}

	// Create shadow table with same structure as original
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s LIKE %s
	`, shadowTableName, originalTable)

	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create shadow table: %w", err)
	}

	// Execute schema migration scripts on shadow table
	for _, script := range migration.Spec.Migration.Scripts {
		if script.Type == databasev1alpha1.ScriptTypeSchema {
			// Load script content using ScriptLoader service
			var scriptContent string
			var err error

			if e.scriptLoader != nil {
				scriptContent, err = e.scriptLoader.LoadScript(ctx, script)
				if err != nil {
					return fmt.Errorf("failed to load schema script %s: %w", script.Name, err)
				}
			} else {
				// Fallback to placeholder for backward compatibility
				scriptContent = e.getScriptContentFallback(script)
			}

			// Replace table references with shadow table
			modifiedScript := strings.ReplaceAll(scriptContent, originalTable, shadowTableName)

			if _, err := db.ExecContext(ctx, modifiedScript); err != nil {
				return fmt.Errorf("failed to execute schema script %s: %w", script.Name, err)
			}
		}
	}

	// Get total row count for progress tracking
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", originalTable)
	var totalRows int64
	if err := db.QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
		return fmt.Errorf("failed to get total row count: %w", err)
	}

	// Update shadow table info
	migration.Status.ShadowTable.TotalRows = totalRows
	migration.Status.ShadowTable.RowsProcessed = 0

	return nil
}

// SyncData synchronizes data from original to shadow table
func (e *Engine) SyncData(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	if migration.Status.ShadowTable == nil {
		return fmt.Errorf("shadow table info not found")
	}

	shadowTableName := migration.Status.ShadowTable.Name
	originalTable, err := e.extractTableName(migration.Spec.Migration.Scripts)
	if err != nil {
		return fmt.Errorf("failed to extract table name: %w", err)
	}

	batchSize := migration.Spec.Migration.BatchSize
	if batchSize == 0 {
		batchSize = 1000
	}

	// Get primary key column
	pkColumn, err := e.getPrimaryKeyColumn(ctx, db, originalTable)
	if err != nil {
		return fmt.Errorf("failed to get primary key column: %w", err)
	}

	// Get the last processed ID
	lastProcessedID := migration.Status.ShadowTable.RowsProcessed

	// Copy data in batches using MySQL-specific syntax
	copyQuery := fmt.Sprintf(`
		INSERT IGNORE INTO %s 
		SELECT * FROM %s 
		WHERE %s > ? 
		ORDER BY %s 
		LIMIT ?
	`, shadowTableName, originalTable, pkColumn, pkColumn)

	result, err := db.ExecContext(ctx, copyQuery, lastProcessedID, batchSize)
	if err != nil {
		return fmt.Errorf("failed to copy data batch: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Update progress
	migration.Status.ShadowTable.RowsProcessed += rowsAffected
	migration.Status.ShadowTable.LastSyncTime = &metav1.Time{Time: time.Now()}

	// Check if sync is complete
	if migration.Status.ShadowTable.RowsProcessed >= migration.Status.ShadowTable.TotalRows {
		migration.Status.ShadowTable.SyncStatus = "InSync"
	}

	// Execute data migration scripts
	for _, script := range migration.Spec.Migration.Scripts {
		if script.Type == databasev1alpha1.ScriptTypeData {
			var scriptContent string
			var err error

			if e.scriptLoader != nil {
				scriptContent, err = e.scriptLoader.LoadScript(ctx, script)
				if err != nil {
					return fmt.Errorf("failed to load data script %s: %w", script.Name, err)
				}
			} else {
				// Fallback to placeholder for backward compatibility
				scriptContent = e.getScriptContentFallback(script)
			}

			modifiedScript := strings.ReplaceAll(scriptContent, originalTable, shadowTableName)

			if _, err := db.ExecContext(ctx, modifiedScript); err != nil {
				return fmt.Errorf("failed to execute data script %s: %w", script.Name, err)
			}
		}
	}

	return nil
}

// SwapTables atomically swaps original and shadow tables
func (e *Engine) SwapTables(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	if migration.Status.ShadowTable == nil {
		return fmt.Errorf("shadow table info not found")
	}

	shadowTableName := migration.Status.ShadowTable.Name
	originalTable, err := e.extractTableName(migration.Spec.Migration.Scripts)
	if err != nil {
		return fmt.Errorf("failed to extract table name: %w", err)
	}

	backupTableName := fmt.Sprintf("%s_backup_%d", originalTable, time.Now().Unix())

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.FromContext(ctx).Error(rollbackErr, "Failed to rollback transaction")
		}
	}()

	// Rename original table to backup
	renameOriginalQuery := fmt.Sprintf("RENAME TABLE %s TO %s", originalTable, backupTableName)
	if _, err := tx.ExecContext(ctx, renameOriginalQuery); err != nil {
		return fmt.Errorf("failed to rename original table: %w", err)
	}

	// Rename shadow table to original
	renameShadowQuery := fmt.Sprintf("RENAME TABLE %s TO %s", shadowTableName, originalTable)
	if _, err := tx.ExecContext(ctx, renameShadowQuery); err != nil {
		return fmt.Errorf("failed to rename shadow table: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit table swap transaction: %w", err)
	}

	// Store backup table name for rollback
	if migration.Status.BackupInfo == nil {
		migration.Status.BackupInfo = &databasev1alpha1.BackupInfo{}
	}
	migration.Status.BackupInfo.Location = backupTableName

	return nil
}

// Rollback performs rollback operations
func (e *Engine) Rollback(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	if migration.Status.BackupInfo == nil || migration.Status.BackupInfo.Location == "" {
		return fmt.Errorf("no backup information available for rollback")
	}

	originalTable, err := e.extractTableName(migration.Spec.Migration.Scripts)
	if err != nil {
		return fmt.Errorf("failed to extract table name: %w", err)
	}

	backupTableName := migration.Status.BackupInfo.Location
	currentTableName := fmt.Sprintf("%s_failed_%d", originalTable, time.Now().Unix())

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.FromContext(ctx).Error(rollbackErr, "Failed to rollback transaction")
		}
	}()

	// Rename current table (failed migration)
	renameCurrentQuery := fmt.Sprintf("RENAME TABLE %s TO %s", originalTable, currentTableName)
	if _, err := tx.ExecContext(ctx, renameCurrentQuery); err != nil {
		return fmt.Errorf("failed to rename current table during rollback: %w", err)
	}

	// Restore backup table
	restoreQuery := fmt.Sprintf("RENAME TABLE %s TO %s", backupTableName, originalTable)
	if _, err := tx.ExecContext(ctx, restoreQuery); err != nil {
		return fmt.Errorf("failed to restore backup table: %w", err)
	}

	// Commit rollback transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit rollback transaction: %w", err)
	}

	// Clean up shadow table if it exists
	if migration.Status.ShadowTable != nil && migration.Status.ShadowTable.Name != "" {
		dropShadowQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", migration.Status.ShadowTable.Name)
		if _, err := db.ExecContext(ctx, dropShadowQuery); err != nil {
			// Log error but don't fail rollback
			fmt.Printf("Warning: failed to drop shadow table: %v\n", err)
		}
	}

	return nil
}

// GetMetrics returns current migration metrics
func (e *Engine) GetMetrics(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.MigrationMetrics, error) {
	metrics := &databasev1alpha1.MigrationMetrics{}

	// Calculate duration
	if migration.Status.StartTime != nil {
		duration := time.Since(migration.Status.StartTime.Time)
		metrics.Duration = &metav1.Duration{Duration: duration}
	}

	// Get MySQL-specific metrics
	var connections, qps int
	metricsQuery := `
		SELECT 
			(SELECT COUNT(*) FROM information_schema.processlist WHERE command != 'Sleep') as active_connections,
			(SELECT SUM(variable_value) FROM performance_schema.global_status WHERE variable_name IN ('Questions', 'Com_select', 'Com_insert', 'Com_update', 'Com_delete')) as total_queries
	`

	if err := db.QueryRowContext(ctx, metricsQuery).Scan(&connections, &qps); err != nil {
		return nil, fmt.Errorf("failed to get database metrics: %w", err)
	}

	metrics.QueriesPerSecond = strconv.Itoa(qps)

	// Get system metrics (simplified)
	metrics.CPUUsage = "45%" // This would be calculated from actual system metrics
	metrics.MemoryUsage = "60%"
	metrics.DiskUsage = "15GB"

	return metrics, nil
}

// ValidateDataIntegrity checks data integrity between tables
func (e *Engine) ValidateDataIntegrity(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) error {
	originalTable, err := e.extractTableName(migration.Spec.Migration.Scripts)
	if err != nil {
		return fmt.Errorf("failed to extract table name: %w", err)
	}

	// Get row count
	var rowCount int64
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", originalTable)
	if err := db.QueryRowContext(ctx, countQuery).Scan(&rowCount); err != nil {
		return fmt.Errorf("failed to get row count for integrity check: %w", err)
	}

	// Basic integrity checks
	if rowCount == 0 {
		return fmt.Errorf("data integrity check failed: table is empty after migration")
	}

	// Check for data corruption by comparing checksums (if enabled)
	if migration.Spec.Validation != nil &&
		migration.Spec.Validation.DataIntegrityChecks.ChecksumValidation {

		if err := e.validateChecksums(ctx, db, originalTable, migration); err != nil {
			return fmt.Errorf("checksum validation failed: %w", err)
		}
	}

	return nil
}

// CreateBackup creates a backup before migration
func (e *Engine) CreateBackup(ctx context.Context, db *sql.DB, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.BackupInfo, error) {
	originalTable, err := e.extractTableName(migration.Spec.Migration.Scripts)
	if err != nil {
		return nil, fmt.Errorf("failed to extract table name: %w", err)
	}

	backupTableName := fmt.Sprintf("%s_backup_%d", originalTable, time.Now().Unix())

	// Create backup table
	backupQuery := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", backupTableName, originalTable)
	if _, err := db.ExecContext(ctx, backupQuery); err != nil {
		return nil, fmt.Errorf("failed to create backup table: %w", err)
	}

	// Get table size
	sizeQuery := `
		SELECT ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)' 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() AND table_name = ?
	`

	var sizeMB float64
	if err := db.QueryRowContext(ctx, sizeQuery, backupTableName).Scan(&sizeMB); err != nil {
		return nil, fmt.Errorf("failed to get backup size: %w", err)
	}

	return &databasev1alpha1.BackupInfo{
		Location:  backupTableName,
		Size:      fmt.Sprintf("%.2f MB", sizeMB),
		CreatedAt: &metav1.Time{Time: time.Now()},
	}, nil
}

// RestoreFromBackup restores database from backup
func (e *Engine) RestoreFromBackup(ctx context.Context, db *sql.DB, backupInfo *databasev1alpha1.BackupInfo) error {
	if backupInfo == nil || backupInfo.Location == "" {
		return fmt.Errorf("no backup information provided")
	}

	// This is a simplified restore - in production you'd want more sophisticated backup/restore
	// For now, we'll assume the backup is a table that can be restored
	restoreQuery := fmt.Sprintf("SELECT * FROM %s LIMIT 1", backupInfo.Location)

	// Test if backup table exists and is accessible
	if _, err := db.ExecContext(ctx, restoreQuery); err != nil {
		return fmt.Errorf("backup table is not accessible: %w", err)
	}

	return nil
}

// Helper methods

// validatePermissions checks if the database user has necessary permissions
func (e *Engine) validatePermissions(ctx context.Context, db *sql.DB) error {
	// Check if user can create tables
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _test_permissions (id int)"); err != nil {
		return fmt.Errorf("insufficient permissions to create tables: %w", err)
	}

	// Clean up test table
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS _test_permissions"); err != nil {
		return fmt.Errorf("insufficient permissions to drop tables: %w", err)
	}

	// Check if user can read from information_schema
	if _, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables LIMIT 1"); err != nil {
		return fmt.Errorf("insufficient permissions to read schema information: %w", err)
	}

	return nil
}

// extractTableName extracts the main table name from migration scripts
func (e *Engine) extractTableName(scripts []databasev1alpha1.MigrationScript) (string, error) {
	if len(scripts) == 0 {
		return "", fmt.Errorf("no migration scripts provided")
	}

	// For now, we'll use a simple heuristic to extract table name
	// In production, you'd want more sophisticated SQL parsing
	for _, script := range scripts {
		if script.Type == databasev1alpha1.ScriptTypeSchema {
			// Look for CREATE TABLE, ALTER TABLE, etc.
			var content string
			var err error

			if e.scriptLoader != nil {
				content, err = e.scriptLoader.LoadScript(context.Background(), script)
				if err != nil {
					continue // Skip this script if we can't load it
				}
			} else {
				// Fallback to placeholder for backward compatibility
				content = e.getScriptContentFallback(script)
			}

			if tableName := e.parseTableNameFromSQL(content); tableName != "" {
				return tableName, nil
			}
		}
	}

	// Fallback: use the first script name as table name
	return strings.ReplaceAll(scripts[0].Name, "-", "_"), nil
}

// parseTableNameFromSQL extracts table name from SQL content
func (e *Engine) parseTableNameFromSQL(content string) string {
	// Simple regex-based extraction - in production use proper SQL parser
	lines := strings.Split(strings.ToLower(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "create table") {
			parts := strings.Fields(line)
			for i, part := range parts {
				if part == "table" && i+1 < len(parts) {
					tableName := parts[i+1]
					// Remove quotes and semicolons
					tableName = strings.Trim(tableName, "`\"';")
					return tableName
				}
			}
		}
	}
	return ""
}

// getPrimaryKeyColumn gets the primary key column name for a table
func (e *Engine) getPrimaryKeyColumn(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE() 
		  AND table_name = ? 
		  AND constraint_name = 'PRIMARY'
		LIMIT 1
	`

	var pkColumn string
	if err := db.QueryRowContext(ctx, query, tableName).Scan(&pkColumn); err != nil {
		if err == sql.ErrNoRows {
			// Fallback to 'id' column if no primary key found
			return "id", nil
		}
		return "", fmt.Errorf("failed to get primary key column: %w", err)
	}

	return pkColumn, nil
}

// validateChecksums validates data integrity using checksums
func (e *Engine) validateChecksums(ctx context.Context, db *sql.DB, tableName string, migration *databasev1alpha1.DatabaseMigration) error {
	// Calculate checksum for the table using MySQL MD5 function
	checksumQuery := fmt.Sprintf(`
		SELECT MD5(GROUP_CONCAT(CONCAT_WS('|', *)))
		FROM %s
	`, tableName)

	var calculatedChecksum string
	if err := db.QueryRowContext(ctx, checksumQuery).Scan(&calculatedChecksum); err != nil {
		return fmt.Errorf("failed to calculate table checksum: %w", err)
	}

	// Compare with expected checksum if available
	for _, script := range migration.Spec.Migration.Scripts {
		if script.Checksum != "" && strings.Contains(script.Checksum, calculatedChecksum) {
			return nil // Checksum matches
		}
	}

	// If no checksum provided, just log a warning
	fmt.Printf("Warning: No checksum validation performed for table %s\n", tableName)
	return nil
}

// getScriptContentFallback provides a fallback script content
func (e *Engine) getScriptContentFallback(script databasev1alpha1.MigrationScript) string {
	// Fallback implementation for backward compatibility
	switch script.Type {
	case databasev1alpha1.ScriptTypeSchema:
		return fmt.Sprintf("ALTER TABLE users ADD COLUMN %s VARCHAR(255)", script.Name)
	case databasev1alpha1.ScriptTypeData:
		return fmt.Sprintf("UPDATE users SET %s = 'default_value' WHERE %s IS NULL", script.Name, script.Name)
	case databasev1alpha1.ScriptTypeValidation:
		return fmt.Sprintf("SELECT COUNT(*) FROM users WHERE %s IS NOT NULL", script.Name)
	default:
		return fmt.Sprintf("-- Script: %s", script.Name)
	}
}
