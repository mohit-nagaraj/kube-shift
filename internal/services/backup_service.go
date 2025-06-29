package services

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
		cloudLocation, err := bs.uploadToCloudStorage(ctx, backupPath, migration)
		if err != nil {
			log.Error(err, "Failed to upload backup to cloud storage, keeping local copy")
			// Don't fail the backup creation if cloud upload fails
		} else {
			storageLocation = cloudLocation
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
	// Look for backup configuration in the migration spec
	if migration.Spec.Rollback != nil && migration.Spec.Rollback.PreserveBackup {
		// Check if there's a backup storage configuration
		// This could be in annotations or a separate backup config
		if storageType, exists := migration.Annotations["backup.storage.type"]; exists {
			return storageType == "s3" || storageType == "gcs" || storageType == "azure"
		}
	}
	return false
}

func (bs *BackupService) uploadToCloudStorage(ctx context.Context, localPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Get storage configuration from annotations
	storageType, exists := migration.Annotations["backup.storage.type"]
	if !exists {
		return localPath, fmt.Errorf("backup storage type not configured")
	}

	bucketName, exists := migration.Annotations["backup.storage.bucket"]
	if !exists {
		return localPath, fmt.Errorf("backup storage bucket not configured")
	}

	// Generate cloud path
	fileName := filepath.Base(localPath)
	cloudPath := fmt.Sprintf("%s://%s/backups/%s/%s", storageType, bucketName, migration.Namespace, fileName)

	switch storageType {
	case "s3":
		return bs.uploadToS3(ctx, localPath, cloudPath, migration)
	case "gcs":
		return bs.uploadToGCS(ctx, localPath, cloudPath, migration)
	case "azure":
		return bs.uploadToAzure(ctx, localPath, cloudPath, migration)
	default:
		return localPath, fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// uploadToS3 uploads backup to AWS S3
func (bs *BackupService) uploadToS3(ctx context.Context, localPath, cloudPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Extract bucket and key from cloud path
	// Format: s3://bucket-name/path/to/file
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return localPath, fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucketAndKey := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndKey) != 2 {
		return localPath, fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucket := bucketAndKey[0]
	key := bucketAndKey[1]

	// Use AWS CLI for S3 upload
	cmd := exec.CommandContext(ctx, "aws", "s3", "cp", localPath, fmt.Sprintf("s3://%s/%s", bucket, key))

	// Set AWS credentials from environment or IAM role
	cmd.Env = append(os.Environ(),
		"AWS_DEFAULT_REGION="+bs.getAWSRegion(migration),
	)

	if err := cmd.Run(); err != nil {
		return localPath, fmt.Errorf("S3 upload failed: %w", err)
	}

	return cloudPath, nil
}

// uploadToGCS uploads backup to Google Cloud Storage
func (bs *BackupService) uploadToGCS(ctx context.Context, localPath, cloudPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Extract bucket and object from cloud path
	// Format: gs://bucket-name/path/to/file
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return localPath, fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucketAndObject := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndObject) != 2 {
		return localPath, fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucket := bucketAndObject[0]
	object := bucketAndObject[1]

	// Use gsutil for GCS upload
	cmd := exec.CommandContext(ctx, "gsutil", "cp", localPath, fmt.Sprintf("gs://%s/%s", bucket, object))

	if err := cmd.Run(); err != nil {
		return localPath, fmt.Errorf("GCS upload failed: %w", err)
	}

	return cloudPath, nil
}

// uploadToAzure uploads backup to Azure Blob Storage
func (bs *BackupService) uploadToAzure(ctx context.Context, localPath, cloudPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Extract container and blob from cloud path
	// Format: az://container-name/path/to/file
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return localPath, fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	containerAndBlob := strings.SplitN(pathParts[1], "/", 2)
	if len(containerAndBlob) != 2 {
		return localPath, fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	container := containerAndBlob[0]
	blob := containerAndBlob[1]

	// Use az cli for Azure upload
	cmd := exec.CommandContext(ctx, "az", "storage", "blob", "upload",
		"--account-name", bs.getAzureStorageAccount(migration),
		"--container-name", container,
		"--name", blob,
		"--file", localPath,
		"--auth-mode", "login",
	)

	if err := cmd.Run(); err != nil {
		return localPath, fmt.Errorf("Azure upload failed: %w", err)
	}

	return cloudPath, nil
}

// getAWSRegion gets AWS region from migration annotations or environment
func (bs *BackupService) getAWSRegion(migration *databasev1alpha1.DatabaseMigration) string {
	if region, exists := migration.Annotations["backup.storage.aws.region"]; exists {
		return region
	}
	return "us-east-1" // Default region
}

// getAzureStorageAccount gets Azure storage account from migration annotations
func (bs *BackupService) getAzureStorageAccount(migration *databasev1alpha1.DatabaseMigration) string {
	if account, exists := migration.Annotations["backup.storage.azure.account"]; exists {
		return account
	}
	return "defaultstorageaccount" // Default account
}

func (bs *BackupService) downloadFromCloudStorage(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Parse cloud path to determine storage type
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return cloudPath, fmt.Errorf("invalid cloud path format: %s", cloudPath)
	}

	storageType := pathParts[0]

	// Create temporary local file
	tempFile, err := os.CreateTemp(bs.backupDir, "backup_download_*")
	if err != nil {
		return cloudPath, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	localPath := tempFile.Name()

	switch storageType {
	case "s3":
		err = bs.downloadFromS3(ctx, cloudPath, localPath, migration)
	case "gs":
		err = bs.downloadFromGCS(ctx, cloudPath, localPath, migration)
	case "az":
		err = bs.downloadFromAzure(ctx, cloudPath, localPath, migration)
	default:
		return cloudPath, fmt.Errorf("unsupported storage type: %s", storageType)
	}

	if err != nil {
		os.Remove(localPath) // Clean up temp file on error
		return cloudPath, err
	}

	return localPath, nil
}

// downloadFromS3 downloads backup from AWS S3
func (bs *BackupService) downloadFromS3(ctx context.Context, cloudPath, localPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Use AWS CLI for S3 download
	cmd := exec.CommandContext(ctx, "aws", "s3", "cp", cloudPath, localPath)

	// Set AWS credentials from environment or IAM role
	cmd.Env = append(os.Environ(),
		"AWS_DEFAULT_REGION="+bs.getAWSRegion(migration),
	)

	return cmd.Run()
}

// downloadFromGCS downloads backup from Google Cloud Storage
func (bs *BackupService) downloadFromGCS(ctx context.Context, cloudPath, localPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Use gsutil for GCS download
	cmd := exec.CommandContext(ctx, "gsutil", "cp", cloudPath, localPath)
	return cmd.Run()
}

// downloadFromAzure downloads backup from Azure Blob Storage
func (bs *BackupService) downloadFromAzure(ctx context.Context, cloudPath, localPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Extract container and blob from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	containerAndBlob := strings.SplitN(pathParts[1], "/", 2)
	if len(containerAndBlob) != 2 {
		return fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	container := containerAndBlob[0]
	blob := containerAndBlob[1]

	// Use az cli for Azure download
	cmd := exec.CommandContext(ctx, "az", "storage", "blob", "download",
		"--account-name", bs.getAzureStorageAccount(migration),
		"--container-name", container,
		"--name", blob,
		"--file", localPath,
		"--auth-mode", "login",
	)

	return cmd.Run()
}

func (bs *BackupService) deleteFromCloudStorage(ctx context.Context, cloudPath string) error {
	// Parse cloud path to determine storage type
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid cloud path format: %s", cloudPath)
	}

	storageType := pathParts[0]

	switch storageType {
	case "s3":
		return bs.deleteFromS3(ctx, cloudPath)
	case "gs":
		return bs.deleteFromGCS(ctx, cloudPath)
	case "az":
		return bs.deleteFromAzure(ctx, cloudPath)
	default:
		return fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// deleteFromS3 deletes backup from AWS S3
func (bs *BackupService) deleteFromS3(ctx context.Context, cloudPath string) error {
	// Use AWS CLI for S3 deletion
	cmd := exec.CommandContext(ctx, "aws", "s3", "rm", cloudPath)
	return cmd.Run()
}

// deleteFromGCS deletes backup from Google Cloud Storage
func (bs *BackupService) deleteFromGCS(ctx context.Context, cloudPath string) error {
	// Use gsutil for GCS deletion
	cmd := exec.CommandContext(ctx, "gsutil", "rm", cloudPath)
	return cmd.Run()
}

// deleteFromAzure deletes backup from Azure Blob Storage
func (bs *BackupService) deleteFromAzure(ctx context.Context, cloudPath string) error {
	// Extract container and blob from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	containerAndBlob := strings.SplitN(pathParts[1], "/", 2)
	if len(containerAndBlob) != 2 {
		return fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	container := containerAndBlob[0]
	blob := containerAndBlob[1]

	// Use az cli for Azure deletion
	cmd := exec.CommandContext(ctx, "az", "storage", "blob", "delete",
		"--container-name", container,
		"--name", blob,
		"--auth-mode", "login",
	)

	return cmd.Run()
}

func (bs *BackupService) validateCloudBackup(ctx context.Context, cloudPath string) error {
	// Parse cloud path to determine storage type
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid cloud path format: %s", cloudPath)
	}

	storageType := pathParts[0]

	switch storageType {
	case "s3":
		return bs.validateS3Backup(ctx, cloudPath)
	case "gs":
		return bs.validateGCSBackup(ctx, cloudPath)
	case "az":
		return bs.validateAzureBackup(ctx, cloudPath)
	default:
		return fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// validateS3Backup validates backup in AWS S3
func (bs *BackupService) validateS3Backup(ctx context.Context, cloudPath string) error {
	// Use AWS CLI to check if object exists
	cmd := exec.CommandContext(ctx, "aws", "s3", "ls", cloudPath)
	return cmd.Run()
}

// validateGCSBackup validates backup in Google Cloud Storage
func (bs *BackupService) validateGCSBackup(ctx context.Context, cloudPath string) error {
	// Use gsutil to check if object exists
	cmd := exec.CommandContext(ctx, "gsutil", "ls", cloudPath)
	return cmd.Run()
}

// validateAzureBackup validates backup in Azure Blob Storage
func (bs *BackupService) validateAzureBackup(ctx context.Context, cloudPath string) error {
	// Extract container and blob from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	containerAndBlob := strings.SplitN(pathParts[1], "/", 2)
	if len(containerAndBlob) != 2 {
		return fmt.Errorf("invalid Azure path format: %s", cloudPath)
	}

	container := containerAndBlob[0]
	blob := containerAndBlob[1]

	// Use az cli to check if blob exists
	cmd := exec.CommandContext(ctx, "az", "storage", "blob", "show",
		"--container-name", container,
		"--name", blob,
		"--auth-mode", "login",
	)

	return cmd.Run()
}

func (bs *BackupService) checkBackupIntegrity(ctx context.Context, backupPath string) error {
	// Check if backup file exists and is readable
	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		return fmt.Errorf("failed to stat backup file: %w", err)
	}

	// Check if file is not empty
	if fileInfo.Size() == 0 {
		return fmt.Errorf("backup file is empty")
	}

	// Check if file is readable
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// Read first few bytes to check if file is not corrupted
	buffer := make([]byte, 1024)
	bytesRead, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	if bytesRead == 0 {
		return fmt.Errorf("backup file appears to be empty or corrupted")
	}

	// Check file header to determine backup type and validate format
	if err := bs.validateBackupFormat(backupPath, buffer[:bytesRead]); err != nil {
		return fmt.Errorf("backup format validation failed: %w", err)
	}

	// Check file permissions
	if fileInfo.Mode()&0400 == 0 {
		return fmt.Errorf("backup file is not readable")
	}

	return nil
}

// validateBackupFormat validates the backup file format based on file header
func (bs *BackupService) validateBackupFormat(backupPath string, header []byte) error {
	fileName := strings.ToLower(filepath.Base(backupPath))

	// Check for common backup file extensions and headers
	if strings.HasSuffix(fileName, ".sql") {
		// SQL backup files should start with common SQL keywords or comments
		content := strings.ToLower(string(header))
		if !strings.Contains(content, "create") &&
			!strings.Contains(content, "insert") &&
			!strings.Contains(content, "--") &&
			!strings.Contains(content, "/*") {
			return fmt.Errorf("invalid SQL backup format")
		}
	} else if strings.HasSuffix(fileName, ".dump") || strings.HasSuffix(fileName, ".backup") {
		// Binary backup files should not be empty and should have some structure
		if len(header) < 10 {
			return fmt.Errorf("backup file too small to be valid")
		}
	} else if strings.HasSuffix(fileName, ".gz") || strings.HasSuffix(fileName, ".gzip") {
		// Compressed files should start with gzip magic number
		if len(header) < 2 || header[0] != 0x1f || header[1] != 0x8b {
			return fmt.Errorf("invalid gzip format")
		}
	} else if strings.HasSuffix(fileName, ".tar") {
		// Tar files should start with tar magic number
		if len(header) < 262 || string(header[257:262]) != "ustar" {
			return fmt.Errorf("invalid tar format")
		}
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

// compressBackup compresses a backup file using gzip
func (bs *BackupService) compressBackup(ctx context.Context, backupPath string) (string, error) {
	compressedPath := backupPath + ".gz"

	// Use gzip to compress the backup
	cmd := exec.CommandContext(ctx, "gzip", "-f", backupPath)
	if err := cmd.Run(); err != nil {
		return backupPath, fmt.Errorf("failed to compress backup: %w", err)
	}

	return compressedPath, nil
}

// encryptBackup encrypts a backup file using GPG
func (bs *BackupService) encryptBackup(ctx context.Context, backupPath string, migration *databasev1alpha1.DatabaseMigration) (string, error) {
	// Get encryption key from annotations or secret
	encryptionKey, exists := migration.Annotations["backup.encryption.key"]
	if !exists {
		return backupPath, fmt.Errorf("encryption key not configured")
	}

	encryptedPath := backupPath + ".gpg"

	// Use GPG to encrypt the backup
	cmd := exec.CommandContext(ctx, "gpg", "--batch", "--yes", "--passphrase", encryptionKey, "-c", backupPath)
	if err := cmd.Run(); err != nil {
		return backupPath, fmt.Errorf("failed to encrypt backup: %w", err)
	}

	// Remove the unencrypted file
	if err := os.Remove(backupPath); err != nil {
		return encryptedPath, fmt.Errorf("failed to remove unencrypted backup: %w", err)
	}

	return encryptedPath, nil
}

// cleanupOldBackups removes old backup files to free up space
func (bs *BackupService) cleanupOldBackups(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error {
	// Get retention policy from annotations
	retentionDays := 7 // Default retention
	if retentionStr, exists := migration.Annotations["backup.retention.days"]; exists {
		if days, err := strconv.Atoi(retentionStr); err == nil && days > 0 {
			retentionDays = days
		}
	}

	// Calculate cutoff time
	cutoffTime := time.Now().AddDate(0, 0, -retentionDays)

	// List backup files in the backup directory
	files, err := os.ReadDir(bs.backupDir)
	if err != nil {
		return fmt.Errorf("failed to read backup directory: %w", err)
	}

	// Remove old backup files
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Check if file is older than retention period
		fileInfo, err := file.Info()
		if err != nil {
			continue
		}

		if fileInfo.ModTime().Before(cutoffTime) {
			filePath := filepath.Join(bs.backupDir, file.Name())
			if err := os.Remove(filePath); err != nil {
				log.FromContext(ctx).Error(err, "Failed to remove old backup file", "file", filePath)
			} else {
				log.FromContext(ctx).Info("Removed old backup file", "file", filePath)
			}
		}
	}

	return nil
}

// getBackupMetadata extracts metadata from backup file
func (bs *BackupService) getBackupMetadata(backupPath string) (map[string]string, error) {
	metadata := make(map[string]string)

	fileInfo, err := os.Stat(backupPath)
	if err != nil {
		return metadata, err
	}

	metadata["size"] = bs.formatFileSize(fileInfo.Size())
	metadata["created"] = fileInfo.ModTime().Format(time.RFC3339)
	metadata["permissions"] = fileInfo.Mode().String()

	// Try to extract database-specific metadata
	if strings.HasSuffix(backupPath, ".sql") {
		if dbInfo, err := bs.extractSQLMetadata(backupPath); err == nil {
			for k, v := range dbInfo {
				metadata[k] = v
			}
		}
	}

	return metadata, nil
}

// extractSQLMetadata extracts metadata from SQL backup files
func (bs *BackupService) extractSQLMetadata(backupPath string) (map[string]string, error) {
	metadata := make(map[string]string)

	file, err := os.Open(backupPath)
	if err != nil {
		return metadata, err
	}
	defer file.Close()

	// Read first 10KB to look for metadata
	buffer := make([]byte, 10240)
	bytesRead, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return metadata, err
	}

	content := string(buffer[:bytesRead])

	// Look for common SQL backup headers
	if strings.Contains(content, "PostgreSQL database dump") {
		metadata["type"] = "postgresql"
	} else if strings.Contains(content, "MySQL dump") {
		metadata["type"] = "mysql"
	} else if strings.Contains(content, "MariaDB dump") {
		metadata["type"] = "mariadb"
	}

	// Extract version information if available
	if versionMatch := strings.Contains(content, "Server version"); versionMatch {
		// Extract version number
		lines := strings.Split(content, "\n")
		for _, line := range lines {
			if strings.Contains(line, "Server version") {
				metadata["version"] = strings.TrimSpace(line)
				break
			}
		}
	}

	return metadata, nil
}
