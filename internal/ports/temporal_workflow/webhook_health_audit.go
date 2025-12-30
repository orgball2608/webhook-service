package temporal_workflow

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"

	"go.temporal.io/api/enums/v1"

	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
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
	worker.RegisterWorkflowWithOptions(
		WebhookHealthAuditChild,
		workflow.RegisterOptions{Name: "WebhookHealthAuditChild"},
	)
	activities := NewWebhookHealthActivities(w.app)
	worker.RegisterActivityWithOptions(
		activities.GetActiveWebhooks,
		activity.RegisterOptions{Name: "GetActiveWebhooks"},
	)
	worker.RegisterActivityWithOptions(
		activities.CalculateSuccessRate,
		activity.RegisterOptions{Name: "CalculateSuccessRate"},
	)
	worker.RegisterActivityWithOptions(
		activities.DisableWebhook,
		activity.RegisterOptions{Name: "DisableWebhook"},
	)
	worker.RegisterActivityWithOptions(
		activities.SendWarningEmail,
		activity.RegisterOptions{Name: "SendWarningEmail"},
	)
	worker.RegisterActivityWithOptions(
		activities.SendEmail,
		activity.RegisterOptions{Name: "SendEmail"},
	)
	worker.RegisterActivityWithOptions(
		activities.FetchWebhooksByIDs,
		activity.RegisterOptions{Name: "FetchWebhooksByIDs"},
	)
}

func (w WebhookHealthAudit) workflow(ctx workflow.Context) error {
	logger := zap.S().Named("WebhookHealthAudit")
	logger.Info("Starting Webhook Health Audit")

	// Pagination loop
	offset := 0
	limit := 500
	batchSize := 100
	for {
		var activeWebhooks []*webhook.Webhook
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
			}),
			"GetActiveWebhooks",
			offset, limit,
		).Get(ctx, &activeWebhooks)
		if err != nil {
			return fmt.Errorf("failed to get active webhooks: %w", err)
		}
		if len(activeWebhooks) == 0 {
			break
		}

		// Chia thành các batch nhỏ để chạy child workflow song song
		for i := 0; i < len(activeWebhooks); i += batchSize {
			end := i + batchSize
			if end > len(activeWebhooks) {
				end = len(activeWebhooks)
			}
			batch := activeWebhooks[i:end]
			batchIDs := make([]string, len(batch))
			for j, wh := range batch {
				batchIDs[j] = wh.Id
			}
			// Gọi child workflow cho mỗi batch, chỉ truyền []string
			childOpts := workflow.ChildWorkflowOptions{
				TaskQueue:         workflow.GetInfo(ctx).TaskQueueName,
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON,
				WorkflowID:        fmt.Sprintf("webhook-health-audit-child-%d-%d-%d", offset, i, end),
			}
			childCtx := workflow.WithChildOptions(ctx, childOpts)
			err := workflow.ExecuteChildWorkflow(childCtx, "WebhookHealthAuditChild", batchIDs).Get(childCtx, nil)
			if err != nil {
				logger.Errorw("Child workflow failed", "offset", offset, "batch", i, "error", err)
			}
		}

		if len(activeWebhooks) < limit {
			break
		}
		offset += limit
	}
	return nil
}

// Child workflow nhận []string (webhook IDs), fetch metadata qua Activity
func WebhookHealthAuditChild(ctx workflow.Context, batchIDs []string) error {
	logger := zap.S().Named("WebhookHealthAuditChild")
	var batch []*webhook.Webhook
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		}),
		"FetchWebhooksByIDs",
		batchIDs,
	).Get(ctx, &batch)
	if err != nil {
		logger.Errorw("Failed to fetch webhooks by IDs", "ids", batchIDs, "error", err)
		return err
	}
	for _, wh := range batch {
		var result SuccessRateResult
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second,
			}),
			"CalculateSuccessRate",
			wh.Id,
		).Get(ctx, &result)
		if err != nil {
			logger.Errorw("Failed to calculate success rate", "webhook_id", wh.Id, "error", err)
			continue
		}
		rate := result.Rate
		total := result.Total
		if total > 600 {
			if rate < 30 {
				err := workflow.ExecuteActivity(
					workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
					}),
					"DisableWebhook",
					wh.Id,
				).Get(ctx, nil)
				if err != nil {
					logger.Errorw("Failed to disable webhook", "webhook_id", wh.Id, "error", err)
				} else {
					err := workflow.ExecuteActivity(
						workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
							StartToCloseTimeout: 30 * time.Second,
						}),
						"SendEmail",
						wh.Id, "Subscription Disabled", fmt.Sprintf("Your webhook has been disabled due to low success rate: %.2f%%", rate),
					).Get(ctx, nil)
					if err != nil {
						logger.Errorw("Failed to send disable email", "webhook_id", wh.Id, "error", err)
					}
				}
			} else if rate < 70 {
				err := workflow.ExecuteActivity(
					workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
					}),
					"SendWarningEmail",
					wh.Id, rate,
				).Get(ctx, nil)
				if err != nil {
					logger.Errorw("Failed to send warning email", "webhook_id", wh.Id, "error", err)
				}
			}
		}
	}
	return nil
}

type WebhookHealthActivities struct {
	app app.Application
}

func NewWebhookHealthActivities(app app.Application) *WebhookHealthActivities {
	return &WebhookHealthActivities{app: app}
}

func (a *WebhookHealthActivities) GetActiveWebhooks(ctx context.Context, offset, limit int) ([]*webhook.Webhook, error) {
	return a.app.GetActiveWebhooksPaginated(ctx, offset, limit)
}

type SuccessRateResult struct {
	Rate  float64
	Total int64
}

func (a *WebhookHealthActivities) CalculateSuccessRate(ctx context.Context, webhookID string) (SuccessRateResult, error) {
	rate, total, err := a.app.CalculateSuccessRate(ctx, webhookID)
	return SuccessRateResult{Rate: rate, Total: total}, err
}

func (a *WebhookHealthActivities) DisableWebhook(ctx context.Context, webhookID string) error {
	return a.app.DisableWebhook(ctx, webhookID)
}

func (a *WebhookHealthActivities) SendWarningEmail(ctx context.Context, webhookID string, rate float64) error {
	fmt.Printf("Sending warning email to webhook %s: Success rate %.2f%% is low\n", webhookID, rate)
	return nil
}

func (a *WebhookHealthActivities) SendEmail(ctx context.Context, webhookID, subject, body string) error {
	fmt.Printf("Sending email to webhook %s: %s - %s\n", webhookID, subject, body)
	return nil
}

func (a *WebhookHealthActivities) FetchWebhooksByIDs(ctx context.Context, ids []string) ([]*webhook.Webhook, error) {
	return a.app.FetchWebhooksByIDs(ctx, ids)
}
