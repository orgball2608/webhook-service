package temporal_workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

type WebhookHealthAudit struct {
	app app.Application
}

func NewWebhookHealthAudit(app app.Application) WebhookHealthAudit {
	return WebhookHealthAudit{app: app}
}

func (w WebhookHealthAudit) Register(worker worker.Worker) {
	worker.RegisterWorkflowWithOptions(
		w.workflow,
		workflow.RegisterOptions{Name: "WebhookHealthAudit"},
	)
	worker.RegisterActivityWithOptions(
		GetActiveWebhooksActivity,
		activity.RegisterOptions{Name: "GetActiveWebhooks"},
	)
	worker.RegisterActivityWithOptions(
		CalculateSuccessRateActivity,
		activity.RegisterOptions{Name: "CalculateSuccessRate"},
	)
	worker.RegisterActivityWithOptions(
		DisableWebhookActivity,
		activity.RegisterOptions{Name: "DisableWebhook"},
	)
	worker.RegisterActivityWithOptions(
		SendWarningEmailActivity,
		activity.RegisterOptions{Name: "SendWarningEmail"},
	)
	worker.RegisterActivityWithOptions(
		SendEmailActivity,
		activity.RegisterOptions{Name: "SendEmail"},
	)
}

func (w WebhookHealthAudit) workflow(ctx workflow.Context) error {
	logger := zap.S().Named("WebhookHealthAudit")
	logger.Info("Starting Webhook Health Audit")

	// Get active webhooks
	var activeWebhooks []*webhook.Webhook
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		}),
		"GetActiveWebhooks",
		w.app,
	).Get(ctx, &activeWebhooks)
	if err != nil {
		return fmt.Errorf("failed to get active webhooks: %w", err)
	}

	// Process each webhook
	for _, wh := range activeWebhooks {
		var rate float64
		var total int64
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
			}),
			"CalculateSuccessRate",
			wh.Id,
		).Get(ctx, &[]interface{}{&rate, &total})
		if err != nil {
			logger.Errorw("Failed to calculate success rate", "webhook_id", wh.Id, "error", err)
			continue
		}

		if total > 600 {
			if rate < 30 {
				// Disable webhook
				err := workflow.ExecuteActivity(
					workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
					}),
					"DisableWebhook",
					w.app, wh.Id,
				).Get(ctx, nil)
				if err != nil {
					logger.Errorw("Failed to disable webhook", "webhook_id", wh.Id, "error", err)
				} else {
					// Send email
					err := workflow.ExecuteActivity(
						workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
							StartToCloseTimeout: 30 * time.Second,
						}),
						"SendEmail",
						w.app, wh.Id, "Subscription Disabled", fmt.Sprintf("Your webhook has been disabled due to low success rate: %.2f%%", rate),
					).Get(ctx, nil)
					if err != nil {
						logger.Errorw("Failed to send disable email", "webhook_id", wh.Id, "error", err)
					}
				}
			} else if rate < 70 {
				// Send warning
				err := workflow.ExecuteActivity(
					workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
					}),
					"SendWarningEmail",
					w.app, wh.Id, rate,
				).Get(ctx, nil)
				if err != nil {
					logger.Errorw("Failed to send warning email", "webhook_id", wh.Id, "error", err)
				}
			}
		}
	}

	return nil
}

func GetActiveWebhooksActivity(ctx context.Context, app app.Application) ([]*webhook.Webhook, error) {
	return app.NotifyEventHandler.GetActiveWebhooks(ctx)
}

func CalculateSuccessRateActivity(ctx context.Context, app app.Application, webhookID string) (float64, int64, error) {
	return app.NotifyEventHandler.CalculateSuccessRate(ctx, webhookID)
}

func DisableWebhookActivity(ctx context.Context, app app.Application, webhookID string) error {
	return app.NotifyEventHandler.DisableWebhook(ctx, webhookID)
}

func SendWarningEmailActivity(_ context.Context, _ app.Application, webhookID string, rate float64) error {
	fmt.Printf("Sending warning email to webhook %s: Success rate %.2f%% is low\n", webhookID, rate)
	return nil
}

func SendEmailActivity(_ context.Context, _ app.Application, webhookID, subject, body string) error {
	fmt.Printf("Sending email to webhook %s: %s - %s\n", webhookID, subject, body)
	return nil
}
