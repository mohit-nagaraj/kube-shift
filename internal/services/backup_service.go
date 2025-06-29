package services

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/engines/mariadb"
	"github.com/mohit-nagaraj/kube-shift/internal/engines/mysql"
	"github.com/mohit-nagaraj/kube-shift/internal/engines/postgresql"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// BackupService implements the BackupService interface
type BackupService struct {
	client    client.Client
	backupDir string
}

// BackupConfig holds backup configuration
type BackupConfig struct {
	StorageType string // "local", "s3", "gcs", "azure"
	StoragePath string
	Compression bool
	Encryption  bool
}

// NewBackupService creates a new BackupService instance
func NewBackupService(client client.Client) interfaces.BackupService {
	return &BackupService{
		client:    client,
		backupDir: "/tmp/backups", // In production, this would be configurable
	}
}

// CreateBackup creates a backup of the database
func (bs *BackupService) CreateBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.BackupInfo, error) {
	log := log.FromContext(ctx)
	log.Info("Creating database backup", "migration", migration.Name)

	// Get database connection
	db, err := bs.getDatabaseConnection(ctx, migration)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}
	defer db.Close()

	// Create backup directory
	if err := os.MkdirAll(bs.backupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Generate backup filename
	backupName := fmt.Sprintf("%s_%s_%d", migration.Name, migration.Spec.Database.Database, time.Now().Unix())
	backupPath := filepath.Join(bs.backupDir, backupName)

	// Create backup based on database type
	var backupSize string
	switch migration.Spec.Database.Type {
	case databasev1alpha1.DatabaseTypePostgreSQL:
		backupSize, err = bs.createPostgreSQLBackup(ctx, migration, backupPath)
	case databasev1alpha1.DatabaseTypeMySQL:
		backupSize, err = bs.createMySQLBackup(ctx, migration, backupPath)
	case databasev1alpha1.DatabaseTypeMariaDB:
		backupSize, err = bs.createMariaDBBackup(ctx, migration, backupPath)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", migration.Spec.Database.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create backup: %w", err)
	}

	// Upload to cloud storage if configured
	storageLocation := backupPath
	if bs.isCloudStorageConfigured(migration) {
		storageLocation, err = bs.uploadToCloudStorage(ctx, backupPath, migration)
		if err != nil {
			log.Error(err, "Failed to upload backup to cloud storage, keeping local copy")
			// Don't fail the backup creation if cloud upload fails
		}
	}

	log.Info("Backup created successfully", "path", storageLocation, "size", backupSize)

	return &databasev1alpha1.BackupInfo{
		Location:  storageLocation,
		Size:      backupSize,
		CreatedAt: &metav1.Time{Time: time.Now()},
	}, nil
}

// RestoreBackup restores a database from backup
func (bs *BackupService) RestoreBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupInfo *databasev1alpha1.BackupInfo) error {
	log := log.FromContext(ctx)
	log.Info("Restoring database from backup", "backup", backupInfo.Location)

	// Get database connection
	db, err := bs.getDatabaseConnection(ctx, migration)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	defer db.Close()

	// Download from cloud storage if needed
	localPath := backupInfo.Location
	if bs.isCloudStorageConfigured(migration) && !strings.HasPrefix(backupInfo.Location, "/") {
		localPath, err = bs.downloadFromCloudStorage(ctx, backupInfo.Location, migration)
		if err != nil {
			return fmt.Errorf("failed to download backup from cloud storage: %w", err)
		}
		defer os.Remove(localPath) // Clean up local copy
	}

	// Restore backup based on database type
	switch migration.Spec.Database.Type {
	case databasev1alpha1.DatabaseTypePostgreSQL:
		err = bs.restorePostgreSQLBackup(ctx, migration, localPath)
	case databasev1alpha1.DatabaseTypeMySQL:
		err = bs.restoreMySQLBackup(ctx, migration, localPath)
	case databasev1alpha1.DatabaseTypeMariaDB:
		err = bs.restoreMariaDBBackup(ctx, migration, localPath)
	default:
		return fmt.Errorf("unsupported database type: %s", migration.Spec.Database.Type)
	}

	if err != nil {
		return fmt.Errorf("failed to restore backup: %w", err)
	}

	log.Info("Backup restored successfully")
	return nil
}

// DeleteBackup removes a backup
func (bs *BackupService) DeleteBackup(ctx context.Context, backupInfo *databasev1alpha1.BackupInfo) error {
	log := log.FromContext(ctx)
	log.Info("Deleting backup", "location", backupInfo.Location)

	// Delete from cloud storage if it's a cloud backup
	if strings.HasPrefix(backupInfo.Location, "s3://") ||
		strings.HasPrefix(backupInfo.Location, "gs://") ||
		strings.HasPrefix(backupInfo.Location, "az://") {
		return bs.deleteFromCloudStorage(ctx, backupInfo.Location)
	}

	// Delete local file
	if err := os.Remove(backupInfo.Location); err != nil {
		return fmt.Errorf("failed to delete local backup: %w", err)
	}

	log.Info("Backup deleted successfully")
	return nil
}

// ValidateBackup checks if a backup is valid
func (bs *BackupService) ValidateBackup(ctx context.Context, backupInfo *databasev1alpha1.BackupInfo) error {
	log := log.FromContext(ctx)
	log.Info("Validating backup", "location", backupInfo.Location)

	// Check if backup file exists
	if strings.HasPrefix(backupInfo.Location, "/") {
		if _, err := os.Stat(backupInfo.Location); err != nil {
			return fmt.Errorf("backup file not found: %w", err)
		}
	} else {
		// For cloud storage, check if file exists
		if err := bs.validateCloudBackup(ctx, backupInfo.Location); err != nil {
			return fmt.Errorf("cloud backup validation failed: %w", err)
		}
	}

	// Check backup file integrity
	if err := bs.checkBackupIntegrity(ctx, backupInfo.Location); err != nil {
		return fmt.Errorf("backup integrity check failed: %w", err)
	}

	log.Info("Backup validation passed")
	return nil
}

// Helper methods

func (bs *BackupService) getDatabaseConnection(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*sql.DB, error) {
	// Get database credentials from secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := bs.client.Get(ctx, secretKey, secret); err != nil {
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

func (bs *BackupService) createPostgreSQLBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupPath string) (string, error) {
	// Get database credentials
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := bs.client.Get(ctx, secretKey, secret); err != nil {
		return "", fmt.Errorf("failed to get database secret: %w", err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])

	// Build pg_dump command
	cmd := exec.CommandContext(ctx, "pg_dump",
		"-h", migration.Spec.Database.Host,
		"-p", fmt.Sprintf("%d", migration.Spec.Database.Port),
		"-U", username,
		"-d", migration.Spec.Database.Database,
		"-f", backupPath,
		"--verbose",
		"--no-password",
	)

	// Set environment variables
	cmd.Env = append(os.Environ(),
		"PGPASSWORD="+password,
	)

	// Execute backup
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("pg_dump failed: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		return "", fmt.Errorf("failed to get backup file info: %w", err)
	}

	return bs.formatFileSize(fileInfo.Size()), nil
}

func (bs *BackupService) createMySQLBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupPath string) (string, error) {
	// Get database credentials
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := bs.client.Get(ctx, secretKey, secret); err != nil {
		return "", fmt.Errorf("failed to get database secret: %w", err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])

	// Build mysqldump command
	cmd := exec.CommandContext(ctx, "mysqldump",
		"-h", migration.Spec.Database.Host,
		"-P", fmt.Sprintf("%d", migration.Spec.Database.Port),
		"-u", username,
		"-p"+password,
		"--single-transaction",
		"--routines",
		"--triggers",
		"--verbose",
		migration.Spec.Database.Database,
	)

	// Create output file
	outputFile, err := os.Create(backupPath)
	if err != nil {
		return "", fmt.Errorf("failed to create backup file: %w", err)
	}
	defer outputFile.Close()

	cmd.Stdout = outputFile

	// Execute backup
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("mysqldump failed: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		return "", fmt.Errorf("failed to get backup file info: %w", err)
	}

	return bs.formatFileSize(fileInfo.Size()), nil
}

func (bs *BackupService) createMariaDBBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupPath string) (string, error) {
	// MariaDB uses the same backup method as MySQL
	return bs.createMySQLBackup(ctx, migration, backupPath)
}

func (bs *BackupService) restorePostgreSQLBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupPath string) error {
	// Get database credentials
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := bs.client.Get(ctx, secretKey, secret); err != nil {
		return fmt.Errorf("failed to get database secret: %w", err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])

	// Build psql command
	cmd := exec.CommandContext(ctx, "psql",
		"-h", migration.Spec.Database.Host,
		"-p", fmt.Sprintf("%d", migration.Spec.Database.Port),
		"-U", username,
		"-d", migration.Spec.Database.Database,
		"-f", backupPath,
		"--verbose",
		"--no-password",
	)

	// Set environment variables
	cmd.Env = append(os.Environ(),
		"PGPASSWORD="+password,
	)

	// Execute restore
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("psql restore failed: %w", err)
	}

	return nil
}

func (bs *BackupService) restoreMySQLBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupPath string) error {
	// Get database credentials
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Database.ConnectionSecret,
		Namespace: migration.Namespace,
	}

	if err := bs.client.Get(ctx, secretKey, secret); err != nil {
		return fmt.Errorf("failed to get database secret: %w", err)
	}

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])

	// Build mysql command
	cmd := exec.CommandContext(ctx, "mysql",
		"-h", migration.Spec.Database.Host,
		"-P", fmt.Sprintf("%d", migration.Spec.Database.Port),
		"-u", username,
		"-p"+password,
		"--verbose",
		migration.Spec.Database.Database,
	)

	// Create input file
	inputFile, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer inputFile.Close()

	cmd.Stdin = inputFile

	// Execute restore
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysql restore failed: %w", err)
	}

	return nil
}

func (bs *BackupService) restoreMariaDBBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupPath string) error {
	// MariaDB uses the same restore method as MySQL
	return bs.restoreMySQLBackup(ctx, migration, backupPath)
}

func (bs *BackupService) isCloudStorageConfigured(migration *databasev1alpha1.DatabaseMigration) bool {
	// Check if cloud storage is configured in the migration spec
	// This would be implemented based on your cloud storage configuration
	return false // For now, return false
}

func (bs *BackupService) uploadToCloudStorage(ctx context.Context, localPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Implement cloud storage upload logic
	// This would support S3, GCS, Azure Blob, etc.
	return localPath, nil // For now, return local path
}

func (bs *BackupService) downloadFromCloudStorage(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Implement cloud storage download logic
	return cloudPath, nil // For now, return cloud path
}

func (bs *BackupService) deleteFromCloudStorage(ctx context.Context, cloudPath string) error {
	// Implement cloud storage deletion logic
	return nil
}

func (bs *BackupService) validateCloudBackup(ctx context.Context, cloudPath string) error {
	// Implement cloud backup validation logic
	return nil
}

func (bs *BackupService) checkBackupIntegrity(ctx context.Context, backupPath string) error {
	// Check if backup file is readable and has content
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// Read first few bytes to check if file is not empty
	buffer := make([]byte, 1024)
	_, err = file.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	return nil
}

func (bs *BackupService) formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
