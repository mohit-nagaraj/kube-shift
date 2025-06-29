package services

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

	// AWS SDK imports
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	// Google Cloud SDK imports
	"cloud.google.com/go/storage"

	// Azure SDK imports
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// BackupService implements the BackupService interface
type BackupService struct {
	client    client.Client
	backupDir string
}

// Storage type constants
const (
	StorageTypeS3    = "s3"
	StorageTypeGCS   = "gcs"
	StorageTypeAzure = "azure"
)

// BackupConfig holds backup configuration
type BackupConfig struct {
	StorageType string // "local", "s3", "gcs", "azure"
	StoragePath string
	Compression bool
	Encryption  bool
}

// NewBackupService creates a new BackupService instance
func NewBackupService(k8sClient client.Client) interfaces.BackupService {
	return &BackupService{
		client:    k8sClient,
		backupDir: "/tmp/backups", // In production, this would be configurable
	}
}

// CreateBackup creates a backup of the database
func (bs *BackupService) CreateBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) (*databasev1alpha1.BackupInfo, error) {
	logger := log.FromContext(ctx)
	logger.Info("Creating database backup", "migration", migration.Name)

	// Get database connection
	db, err := bs.getDatabaseConnection(ctx, migration)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			logger.Error(closeErr, "Failed to close database connection")
		}
	}()

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
			logger.Error(err, "Failed to upload backup to cloud storage, keeping local copy")
			// Don't fail the backup creation if cloud upload fails
		} else {
			storageLocation = cloudLocation
		}
	}

	logger.Info("Backup created successfully", "path", storageLocation, "size", backupSize)

	return &databasev1alpha1.BackupInfo{
		Location:  storageLocation,
		Size:      backupSize,
		CreatedAt: &metav1.Time{Time: time.Now()},
	}, nil
}

// RestoreBackup restores a database from backup
func (bs *BackupService) RestoreBackup(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, backupInfo *databasev1alpha1.BackupInfo) error {
	logger := log.FromContext(ctx)
	logger.Info("Restoring database from backup", "backup", backupInfo.Location)

	// Get database connection
	db, err := bs.getDatabaseConnection(ctx, migration)
	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			logger.Error(closeErr, "Failed to close database connection")
		}
	}()

	// Download from cloud storage if needed
	localPath := backupInfo.Location
	if bs.isCloudStorageConfigured(migration) && !strings.HasPrefix(backupInfo.Location, "/") {
		localPath, err = bs.downloadFromCloudStorage(ctx, backupInfo.Location, migration)
		if err != nil {
			return fmt.Errorf("failed to download backup from cloud storage: %w", err)
		}
		defer func() {
			if removeErr := os.Remove(localPath); removeErr != nil {
				logger.Error(removeErr, "Failed to remove local backup copy", "path", localPath)
			}
		}() // Clean up local copy
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

	logger.Info("Backup restored successfully")
	return nil
}

// DeleteBackup removes a backup
func (bs *BackupService) DeleteBackup(ctx context.Context, backupInfo *databasev1alpha1.BackupInfo, migration *databasev1alpha1.DatabaseMigration) error {
	logger := log.FromContext(ctx)
	logger.Info("Deleting backup", "location", backupInfo.Location)

	// Delete from cloud storage if it's a cloud backup
	if strings.HasPrefix(backupInfo.Location, "s3://") ||
		strings.HasPrefix(backupInfo.Location, "gs://") ||
		strings.HasPrefix(backupInfo.Location, "az://") {
		return bs.deleteFromCloudStorage(ctx, backupInfo.Location, migration)
	}

	// Delete local file
	if err := os.Remove(backupInfo.Location); err != nil {
		return fmt.Errorf("failed to delete local backup: %w", err)
	}

	logger.Info("Backup deleted successfully")
	return nil
}

// ValidateBackup checks if a backup is valid
func (bs *BackupService) ValidateBackup(ctx context.Context, backupInfo *databasev1alpha1.BackupInfo, migration *databasev1alpha1.DatabaseMigration) error {
	logger := log.FromContext(ctx)
	logger.Info("Validating backup", "location", backupInfo.Location)

	// Check if backup file exists
	if strings.HasPrefix(backupInfo.Location, "/") {
		if _, err := os.Stat(backupInfo.Location); err != nil {
			return fmt.Errorf("backup file not found: %w", err)
		}
	} else {
		// For cloud storage, check if file exists
		if err := bs.validateCloudBackup(ctx, backupInfo.Location, migration); err != nil {
			return fmt.Errorf("cloud backup validation failed: %w", err)
		}
	}

	// Check backup file integrity
	if err := bs.checkBackupIntegrity(ctx, backupInfo.Location); err != nil {
		return fmt.Errorf("backup integrity check failed: %w", err)
	}

	logger.Info("Backup validation passed")
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
	defer func() {
		if closeErr := outputFile.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close output file", "path", backupPath)
		}
	}()

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
	defer func() {
		if closeErr := inputFile.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close input file", "path", backupPath)
		}
	}()

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
			return storageType == StorageTypeS3 || storageType == StorageTypeGCS || storageType == StorageTypeAzure
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
	case StorageTypeS3:
		return bs.uploadToS3(ctx, localPath, cloudPath, migration)
	case StorageTypeGCS:
		return bs.uploadToGCS(ctx, localPath, cloudPath, migration)
	case StorageTypeAzure:
		return bs.uploadToAzure(ctx, localPath, cloudPath, migration)
	default:
		return localPath, fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// uploadToS3 uploads backup to AWS S3 using AWS SDK v2
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

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(bs.getAWSRegion(migration)))
	if err != nil {
		return localPath, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Open local file
	file, err := os.Open(localPath)
	if err != nil {
		return localPath, fmt.Errorf("failed to open local file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close local file", "path", localPath)
		}
	}()

	// Upload to S3
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})

	if err != nil {
		return localPath, fmt.Errorf("S3 upload failed: %w", err)
	}

	return cloudPath, nil
}

// uploadToGCS uploads backup to Google Cloud Storage using GCS SDK
func (bs *BackupService) uploadToGCS(ctx context.Context, localPath, cloudPath string, _ *databasev1alpha1.DatabaseMigration) (string, error) {
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

	// Create GCS client
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return localPath, fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer func() {
		if closeErr := gcsClient.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close GCS client")
		}
	}()

	// Open local file
	file, err := os.Open(localPath)
	if err != nil {
		return localPath, fmt.Errorf("failed to open local file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close local file", "path", localPath)
		}
	}()

	// Get bucket and create object writer
	bkt := gcsClient.Bucket(bucket)
	obj := bkt.Object(object)
	writer := obj.NewWriter(ctx)

	// Copy file to GCS
	if _, err := io.Copy(writer, file); err != nil {
		return localPath, fmt.Errorf("failed to copy file to GCS: %w", err)
	}

	// Close writer to finalize upload
	if err := writer.Close(); err != nil {
		return localPath, fmt.Errorf("failed to finalize GCS upload: %w", err)
	}

	return cloudPath, nil
}

// uploadToAzure uploads backup to Azure Blob Storage using Azure SDK
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

	// Create Azure client
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return localPath, fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Create blob client
	accountName := bs.getAzureStorageAccount(migration)
	url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	blobClient, err := azblob.NewClient(url, cred, nil)
	if err != nil {
		return localPath, fmt.Errorf("failed to create Azure blob client: %w", err)
	}

	// Open local file
	file, err := os.Open(localPath)
	if err != nil {
		return localPath, fmt.Errorf("failed to open local file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close local file", "path", localPath)
		}
	}()

	// Upload to Azure Blob Storage
	_, err = blobClient.UploadFile(ctx, container, blob, file, &azblob.UploadFileOptions{})
	if err != nil {
		return localPath, fmt.Errorf("azure upload failed: %w", err)
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
	defer func() {
		if closeErr := tempFile.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close temp file")
		}
	}()

	localPath := tempFile.Name()

	switch storageType {
	case StorageTypeS3:
		err = bs.downloadFromS3(ctx, cloudPath, localPath, migration)
	case StorageTypeGCS:
		err = bs.downloadFromGCS(ctx, cloudPath, localPath, migration)
	case StorageTypeAzure:
		err = bs.downloadFromAzure(ctx, cloudPath, localPath, migration)
	default:
		return cloudPath, fmt.Errorf("unsupported storage type: %s", storageType)
	}

	if err != nil {
		if removeErr := os.Remove(localPath); removeErr != nil {
			log.FromContext(ctx).Error(removeErr, "Failed to remove temp file on error", "path", localPath)
		} // Clean up temp file on error
		return cloudPath, err
	}

	return localPath, nil
}

// downloadFromS3 downloads backup from AWS S3 using AWS SDK v2
func (bs *BackupService) downloadFromS3(ctx context.Context, cloudPath, localPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Extract bucket and key from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucketAndKey := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndKey) != 2 {
		return fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucket := bucketAndKey[0]
	key := bucketAndKey[1]

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(bs.getAWSRegion(migration)))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Download from S3
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer func() {
		if closeErr := result.Body.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close S3 result body")
		}
	}()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close local file", "path", localPath)
		}
	}()

	// Copy data to local file
	_, err = io.Copy(file, result.Body)
	if err != nil {
		return fmt.Errorf("failed to copy S3 data: %w", err)
	}

	return nil
}

// downloadFromGCS downloads backup from Google Cloud Storage using GCS SDK
func (bs *BackupService) downloadFromGCS(ctx context.Context, cloudPath, localPath string, _ *databasev1alpha1.DatabaseMigration) error {
	// Extract bucket and object from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucketAndObject := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndObject) != 2 {
		return fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucket := bucketAndObject[0]
	object := bucketAndObject[1]

	// Create GCS client
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer func() {
		if closeErr := gcsClient.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close GCS client")
		}
	}()

	// Get bucket and object reader
	bkt := gcsClient.Bucket(bucket)
	obj := bkt.Object(object)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS object reader: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close GCS reader")
		}
	}()

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close local file", "path", localPath)
		}
	}()

	// Copy data to local file
	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("failed to copy GCS data: %w", err)
	}

	return nil
}

// downloadFromAzure downloads backup from Azure Blob Storage using Azure SDK
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

	// Create Azure client
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Create blob client
	accountName := bs.getAzureStorageAccount(migration)
	url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	blobClient, err := azblob.NewClient(url, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure blob client: %w", err)
	}

	// Create local file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close local file", "path", localPath)
		}
	}()

	// Download from Azure Blob Storage
	_, err = blobClient.DownloadFile(ctx, container, blob, file, &azblob.DownloadFileOptions{})
	if err != nil {
		return fmt.Errorf("azure download failed: %w", err)
	}

	// Check if download was successful by checking file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	if fileInfo.Size() == 0 {
		return fmt.Errorf("downloaded file is empty")
	}

	return nil
}

func (bs *BackupService) deleteFromCloudStorage(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Parse cloud path to determine storage type
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid cloud path format: %s", cloudPath)
	}

	storageType := pathParts[0]

	switch storageType {
	case "s3":
		return bs.deleteFromS3(ctx, cloudPath, migration)
	case "gcs":
		return bs.deleteFromGCS(ctx, cloudPath, migration)
	case "azure":
		return bs.deleteFromAzure(ctx, cloudPath, migration)
	default:
		return fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// deleteFromS3 deletes backup from AWS S3 using AWS SDK v2
func (bs *BackupService) deleteFromS3(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Extract bucket and key from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucketAndKey := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndKey) != 2 {
		return fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucket := bucketAndKey[0]
	key := bucketAndKey[1]

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(bs.getAWSRegion(migration)))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Delete from S3
	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("S3 deletion failed: %w", err)
	}

	return nil
}

// deleteFromGCS deletes backup from Google Cloud Storage using GCS SDK
func (bs *BackupService) deleteFromGCS(ctx context.Context, cloudPath string, _ *databasev1alpha1.DatabaseMigration) error {
	// Extract bucket and object from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucketAndObject := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndObject) != 2 {
		return fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucket := bucketAndObject[0]
	object := bucketAndObject[1]

	// Create GCS client
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer func() {
		if closeErr := gcsClient.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close GCS client")
		}
	}()

	// Delete object from GCS
	bkt := gcsClient.Bucket(bucket)
	obj := bkt.Object(object)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("GCS deletion failed: %w", err)
	}

	return nil
}

// deleteFromAzure deletes backup from Azure Blob Storage using Azure SDK
func (bs *BackupService) deleteFromAzure(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) error {
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

	// Create Azure client
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Create blob client
	accountName := bs.getAzureStorageAccount(migration)
	url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	blobClient, err := azblob.NewClient(url, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure blob client: %w", err)
	}

	// Delete from Azure Blob Storage
	_, err = blobClient.DeleteBlob(ctx, container, blob, &azblob.DeleteBlobOptions{})
	if err != nil {
		return fmt.Errorf("azure deletion failed: %w", err)
	}

	return nil
}

func (bs *BackupService) validateCloudBackup(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Parse cloud path to determine storage type
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid cloud path format: %s", cloudPath)
	}

	storageType := pathParts[0]

	switch storageType {
	case StorageTypeS3:
		return bs.validateS3Backup(ctx, cloudPath, migration)
	case StorageTypeGCS:
		return bs.validateGCSBackup(ctx, cloudPath, migration)
	case StorageTypeAzure:
		return bs.validateAzureBackup(ctx, cloudPath, migration)
	default:
		return fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// validateS3Backup validates backup in AWS S3 using AWS SDK v2
func (bs *BackupService) validateS3Backup(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) error {
	// Extract bucket and key from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucketAndKey := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndKey) != 2 {
		return fmt.Errorf("invalid S3 path format: %s", cloudPath)
	}

	bucket := bucketAndKey[0]
	key := bucketAndKey[1]

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(bs.getAWSRegion(migration)))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Check if object exists
	_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("S3 object not found: %w", err)
	}

	return nil
}

// validateGCSBackup validates backup in Google Cloud Storage using GCS SDK
func (bs *BackupService) validateGCSBackup(ctx context.Context, cloudPath string, _ *databasev1alpha1.DatabaseMigration) error {
	// Extract bucket and object from cloud path
	pathParts := strings.SplitN(cloudPath, "://", 2)
	if len(pathParts) != 2 {
		return fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucketAndObject := strings.SplitN(pathParts[1], "/", 2)
	if len(bucketAndObject) != 2 {
		return fmt.Errorf("invalid GCS path format: %s", cloudPath)
	}

	bucket := bucketAndObject[0]
	object := bucketAndObject[1]

	// Create GCS client
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer func() {
		if closeErr := gcsClient.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close GCS client")
		}
	}()

	// Check if object exists
	bkt := gcsClient.Bucket(bucket)
	obj := bkt.Object(object)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("GCS object not found: %w", err)
	}

	// Check if object has content
	if attrs.Size == 0 {
		return fmt.Errorf("GCS object is empty")
	}

	return nil
}

// validateAzureBackup validates backup in Azure Blob Storage using Azure SDK
func (bs *BackupService) validateAzureBackup(ctx context.Context, cloudPath string, migration *databasev1alpha1.DatabaseMigration) error {
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

	// Create Azure client
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure credential: %w", err)
	}

	// Create blob client
	accountName := bs.getAzureStorageAccount(migration)
	url := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	blobClient, err := azblob.NewClient(url, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure blob client: %w", err)
	}

	// Create temporary file for validation
	tempFile, err := os.CreateTemp(bs.backupDir, "backup_validate_*")
	if err != nil {
		return fmt.Errorf("failed to create temp file for validation: %w", err)
	}
	defer func() {
		if closeErr := tempFile.Close(); closeErr != nil {
			log.FromContext(ctx).Error(closeErr, "Failed to close temp file")
		}
		if removeErr := os.Remove(tempFile.Name()); removeErr != nil {
			log.FromContext(ctx).Error(removeErr, "Failed to remove temp file")
		}
	}()

	// Try to download just the first byte to check if blob exists
	_, err = blobClient.DownloadFile(ctx, container, blob, tempFile, &azblob.DownloadFileOptions{
		Range: azblob.HTTPRange{
			Offset: 0,
			Count:  1,
		},
	})
	if err != nil {
		return fmt.Errorf("azure blob not found: %w", err)
	}

	return nil
}

func (bs *BackupService) checkBackupIntegrity(_ context.Context, backupPath string) error {
	// Open backup file
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(context.Background()).Error(closeErr, "Failed to close backup file", "path", backupPath)
		}
	}()

	// Read first few bytes to check file format
	header := make([]byte, 512)
	n, err := file.Read(header)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read backup file header: %w", err)
	}

	if n == 0 {
		return fmt.Errorf("backup file is empty")
	}

	// Validate backup format based on file extension
	if err := bs.validateBackupFormat(backupPath, header[:n]); err != nil {
		return fmt.Errorf("invalid backup format: %w", err)
	}

	// Calculate file checksum for integrity
	if err := bs.calculateFileChecksum(backupPath); err != nil {
		return fmt.Errorf("failed to calculate file checksum: %w", err)
	}

	return nil
}

func (bs *BackupService) validateBackupFormat(backupPath string, header []byte) error {
	ext := strings.ToLower(filepath.Ext(backupPath))

	switch ext {
	case ".sql":
		// Check if it's a valid SQL file
		content := string(header)
		if !strings.Contains(content, "--") && !strings.Contains(content, "/*") {
			return fmt.Errorf("invalid SQL backup format")
		}
	case ".dump", ".backup":
		// Check if it's a PostgreSQL dump
		if len(header) >= 4 && string(header[:4]) == "PGDMP" {
			return nil
		}
		return fmt.Errorf("invalid PostgreSQL dump format")
	default:
		// For other formats, just check if file is not empty
		if len(header) == 0 {
			return fmt.Errorf("backup file appears to be empty")
		}
	}

	return nil
}

func (bs *BackupService) calculateFileChecksum(backupPath string) error {
	file, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open file for checksum: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.FromContext(context.Background()).Error(closeErr, "Failed to close file for checksum", "path", backupPath)
		}
	}()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	checksum := hex.EncodeToString(hash.Sum(nil))
	log.FromContext(context.Background()).Info("Backup checksum calculated", "checksum", checksum)

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
