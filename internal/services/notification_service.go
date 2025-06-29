package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1alpha1 "github.com/mohit-nagaraj/kube-shift/api/v1alpha1"
	"github.com/mohit-nagaraj/kube-shift/internal/interfaces"
)

// NotificationService implements the NotificationService interface
type NotificationService struct {
	client     client.Client
	httpClient *http.Client
}

// SlackMessage represents a Slack message payload
type SlackMessage struct {
	Text        string       `json:"text,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

// Attachment represents a Slack attachment
type Attachment struct {
	Color     string  `json:"color,omitempty"`
	Title     string  `json:"title,omitempty"`
	Text      string  `json:"text,omitempty"`
	Fields    []Field `json:"fields,omitempty"`
	Timestamp int64   `json:"ts,omitempty"`
}

// Field represents a Slack field
type Field struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// EmailMessage represents an email message
type EmailMessage struct {
	From    string
	To      []string
	Subject string
	Body    string
}

// NewNotificationService creates a new NotificationService instance
func NewNotificationService(k8sClient client.Client) interfaces.NotificationService {
	return &NotificationService{
		client: k8sClient,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SendMigrationStarted sends notification when migration starts
func (ns *NotificationService) SendMigrationStarted(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error {
	message := fmt.Sprintf("🚀 Database migration started: %s", migration.Name)
	details := ns.buildMigrationDetails(migration, "started")

	return ns.sendNotifications(ctx, migration, message, details, "info")
}

// SendMigrationCompleted sends notification when migration completes
func (ns *NotificationService) SendMigrationCompleted(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error {
	message := fmt.Sprintf("✅ Database migration completed: %s", migration.Name)
	details := ns.buildMigrationDetails(migration, "completed")

	return ns.sendNotifications(ctx, migration, message, details, "good")
}

// SendMigrationFailed sends notification when migration fails
func (ns *NotificationService) SendMigrationFailed(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, err error) error {
	message := fmt.Sprintf("❌ Database migration failed: %s", migration.Name)
	details := ns.buildMigrationDetails(migration, "failed")
	details += fmt.Sprintf("\nError: %v", err)

	return ns.sendNotifications(ctx, migration, message, details, "danger")
}

// SendRollbackStarted sends notification when rollback starts
func (ns *NotificationService) SendRollbackStarted(ctx context.Context, migration *databasev1alpha1.DatabaseMigration) error {
	message := fmt.Sprintf("🔄 Database migration rollback started: %s", migration.Name)
	details := ns.buildMigrationDetails(migration, "rollback")

	return ns.sendNotifications(ctx, migration, message, details, "warning")
}

// Helper methods

func (ns *NotificationService) sendNotifications(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, message, details, color string) error {
	logger := log.FromContext(ctx)

	if migration.Spec.Notifications == nil {
		return nil
	}

	// Send Slack notification
	if migration.Spec.Notifications.Slack != nil {
		if err := ns.sendSlackNotification(ctx, migration, message, details, color); err != nil {
			logger.Error(err, "Failed to send Slack notification")
			// Don't fail the entire notification process for Slack errors
		}
	}

	// Send email notification
	if migration.Spec.Notifications.Email != nil {
		if err := ns.sendEmailNotification(ctx, migration, message, details); err != nil {
			logger.Error(err, "Failed to send email notification")
			// Don't fail the entire notification process for email errors
		}
	}

	return nil
}

func (ns *NotificationService) sendSlackNotification(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, message, details, color string) error {
	// Get Slack webhook URL from secret
	webhookSecret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      migration.Spec.Notifications.Slack.Webhook,
		Namespace: migration.Namespace,
	}

	if err := ns.client.Get(ctx, secretKey, webhookSecret); err != nil {
		return fmt.Errorf("failed to get Slack webhook secret: %w", err)
	}

	webhookURL, exists := webhookSecret.Data["webhook-url"]
	if !exists {
		return fmt.Errorf("webhook-url not found in secret %s", migration.Spec.Notifications.Slack.Webhook)
	}

	// Build Slack message
	slackMsg := SlackMessage{
		Text: message,
		Attachments: []Attachment{
			{
				Color:     color,
				Title:     fmt.Sprintf("Migration: %s", migration.Name),
				Text:      details,
				Timestamp: time.Now().Unix(),
				Fields: []Field{
					{
						Title: "Namespace",
						Value: migration.Namespace,
						Short: true,
					},
					{
						Title: "Database",
						Value: string(migration.Spec.Database.Type),
						Short: true,
					},
					{
						Title: "Strategy",
						Value: string(migration.Spec.Migration.Strategy),
						Short: true,
					},
					{
						Title: "Phase",
						Value: string(migration.Status.Phase),
						Short: true,
					},
				},
			},
		},
	}

	// Add progress information if available
	if migration.Status.Progress != nil {
		slackMsg.Attachments[0].Fields = append(slackMsg.Attachments[0].Fields, Field{
			Title: "Progress",
			Value: fmt.Sprintf("%d/%d (%d%%)",
				migration.Status.Progress.CompletedSteps,
				migration.Status.Progress.TotalSteps,
				migration.Status.Progress.PercentageComplete),
			Short: true,
		})
	}

	// Send to Slack
	jsonData, err := json.Marshal(slackMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal Slack message: %w", err)
	}

	resp, err := ns.httpClient.Post(string(webhookURL), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to send Slack notification: %w", err)
	}
	defer func() {
		logger := log.FromContext(ctx)
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(closeErr, "Failed to close response body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack API returned status code: %d", resp.StatusCode)
	}

	return nil
}

func (ns *NotificationService) sendEmailNotification(ctx context.Context, migration *databasev1alpha1.DatabaseMigration, subject, body string) error {
	// Get email configuration from secret
	emailSecret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      "email-config", // This would be configurable
		Namespace: migration.Namespace,
	}

	if err := ns.client.Get(ctx, secretKey, emailSecret); err != nil {
		return fmt.Errorf("failed to get email configuration secret: %w", err)
	}

	// Extract email configuration
	smtpHost, exists := emailSecret.Data["smtp-host"]
	if !exists {
		return fmt.Errorf("smtp-host not found in email configuration")
	}

	smtpPort, exists := emailSecret.Data["smtp-port"]
	if !exists {
		return fmt.Errorf("smtp-port not found in email configuration")
	}

	username, exists := emailSecret.Data["username"]
	if !exists {
		return fmt.Errorf("username not found in email configuration")
	}

	password, exists := emailSecret.Data["password"]
	if !exists {
		return fmt.Errorf("password not found in email configuration")
	}

	// Build email message
	emailMsg := EmailMessage{
		From:    string(username),
		To:      migration.Spec.Notifications.Email.Recipients,
		Subject: subject,
		Body:    body,
	}

	// Send email
	return ns.sendEmail(string(smtpHost), string(smtpPort), string(username), string(password), emailMsg)
}

func (ns *NotificationService) sendEmail(smtpHost, smtpPort, username, password string, msg EmailMessage) error {
	// Build email content
	emailBody := fmt.Sprintf("From: %s\r\n", msg.From)
	emailBody += fmt.Sprintf("To: %s\r\n", strings.Join(msg.To, ","))
	emailBody += fmt.Sprintf("Subject: %s\r\n", msg.Subject)
	emailBody += "\r\n"
	emailBody += msg.Body

	// Connect to SMTP server
	auth := smtp.PlainAuth("", username, password, smtpHost)
	addr := fmt.Sprintf("%s:%s", smtpHost, smtpPort)

	// Send email
	err := smtp.SendMail(addr, auth, msg.From, msg.To, []byte(emailBody))
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}

func (ns *NotificationService) buildMigrationDetails(migration *databasev1alpha1.DatabaseMigration, status string) string {
	var details strings.Builder

	details.WriteString("**Migration Details:**\n")
	details.WriteString(fmt.Sprintf("• Name: %s\n", migration.Name))
	details.WriteString(fmt.Sprintf("• Namespace: %s\n", migration.Namespace))
	details.WriteString(fmt.Sprintf("• Database: %s (%s)\n", migration.Spec.Database.Database, migration.Spec.Database.Type))
	details.WriteString(fmt.Sprintf("• Strategy: %s\n", migration.Spec.Migration.Strategy))
	details.WriteString(fmt.Sprintf("• Status: %s\n", status))

	if migration.Status.StartTime != nil {
		details.WriteString(fmt.Sprintf("• Started: %s\n", migration.Status.StartTime.Format(time.RFC3339)))
	}

	if migration.Status.CompletionTime != nil {
		details.WriteString(fmt.Sprintf("• Completed: %s\n", migration.Status.CompletionTime.Format(time.RFC3339)))
	}

	if migration.Status.Progress != nil {
		details.WriteString(fmt.Sprintf("• Progress: %d/%d steps (%d%%)\n",
			migration.Status.Progress.CompletedSteps,
			migration.Status.Progress.TotalSteps,
			migration.Status.Progress.PercentageComplete))
	}

	if migration.Status.Message != "" {
		details.WriteString(fmt.Sprintf("• Message: %s\n", migration.Status.Message))
	}

	return details.String()
}
