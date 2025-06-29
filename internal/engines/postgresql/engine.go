package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// Engine implements the MigrationEngine interface for PostgreSQL
type Engine struct{}

// NewEngine creates a new PostgreSQL migration engine
func NewEngine() interfaces.MigrationEngine {
	return &Engine{}
}

// ValidateConnection checks if we can connect to the database
func (e *Engine) ValidateConnection(ctx context.Context, config databasev1alpha1.DatabaseConfig, credentials map[string]string) error {
	db, err := e.GetConnection(ctx, config, credentials)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

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

	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = "require"
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, username, password, config.Database, sslMode)

	db, err := sql.Open("postgres", dsn)
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
		CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL);
	`, shadowTableName, originalTable)

	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create shadow table: %w", err)
	}

	// Execute schema migration scripts on shadow table
	for _, script := range migration.Spec.Migration.Scripts {
		if script.Type == databasev1alpha1.ScriptTypeSchema {
			// Load script content (this would be implemented by ScriptLoader)
			scriptContent := e.getScriptContent(script)
			
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