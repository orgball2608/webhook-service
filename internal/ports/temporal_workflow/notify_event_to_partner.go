package temporal_workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

var (
	activityNotifyEventToPartner = "ActivityNotifyEventToPartner"
	activityGetWebhook           = "ActivityGetWebhook"
	activityCheckThrottling      = "ActivityCheckThrottling"
)

type NotifyEventToPartner struct {
	app app.App
}

func NewWorkflowNotifyEventToPartner(app app.App) (NotifyEventToPartner, error) {
	return NotifyEventToPartner{
		app: app,
	}, nil
}

func (t *NotifyEventToPartner) Register(temporalWorker worker.Worker) {
	temporalWorker.RegisterWorkflowWithOptions(
		t.workflow,
		workflow.RegisterOptions{Name: app.WorkflowNotifyEvent},
	)
	temporalWorker.RegisterActivityWithOptions(
		t.activity,
		activity.RegisterOptions{Name: activityNotifyEventToPartner},
	)
	temporalWorker.RegisterActivityWithOptions(
		t.getWebhookActivity,
		activity.RegisterOptions{Name: activityGetWebhook},
	)
	temporalWorker.RegisterActivityWithOptions(
		t.checkThrottlingActivity,
		activity.RegisterOptions{Name: activityCheckThrottling},
	)
}

func (t *NotifyEventToPartner) workflow(ctx workflow.Context, e subscriber.Event) error {
	logger := zap.S().With("webhook_id", e.WebhookId)
	logger.Info("workflow NotifyEventToPartner started")

	ctxActivity := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 30,
	})

	var wh *webhook.Webhook
	err := workflow.ExecuteActivity(ctxActivity, activityGetWebhook, e.WebhookId).Get(ctx, &wh)
	if err != nil {
		return err
	}

	err = workflow.UpsertSearchAttributes(ctx, map[string]interface{}{
		common.PartnerIDKey: wh.PartnerId,
		common.WebhookIDKey: wh.Id,
	})
	if err != nil {
		return err
	}

	rate := wh.Metadata.RateLimitPerMinute
	if rate == 0 {
		rate = 100
	}

	for {
		var isThrottled bool
		err := workflow.ExecuteActivity(ctxActivity, activityCheckThrottling, wh.PartnerId, rate).Get(ctx, &isThrottled)
		if err != nil {
			return err
		}
		if !isThrottled {
			break
		}
		var jitterSec int64
		if err := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
			return workflow.Now(ctx).UnixNano() % 30
		}).Get(&jitterSec); err != nil {
			return err
		}
		jitter := time.Duration(jitterSec) * time.Second
		if err := workflow.Sleep(ctx, time.Minute+jitter); err != nil {
			return err
		}
	}

	ctxActivityMain := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 120,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        300 * time.Second,
			BackoffCoefficient:     6.0,
			MaximumInterval:        10800 * time.Second,
			MaximumAttempts:        4,
			NonRetryableErrorTypes: []string{},
		},
	})
	eventPayload, _ := json.Marshal(e)
	redriveEvent := RedriveEvent{
		LogID:              0, // First attempt, no log ID yet
		WebhookID:          e.WebhookId,
		PartnerID:          wh.PartnerId,
		EventPayload:       eventPayload,
		RateLimitPerMinute: rate,
		CurrentTime:        workflow.Now(ctx),
	}
	if err := workflow.ExecuteActivity(ctxActivityMain, activityNotifyEventToPartner, redriveEvent).Get(ctx, nil); err != nil {
		return err
	}
	return nil
}

func isTerminalError(err error) bool {
	var appErr *app.WebhookResponseError
	if errors.As(err, &appErr) {
		if appErr.StatusCode == 400 || appErr.StatusCode == 401 || appErr.StatusCode == 404 {
			return true
		}
	}
	return false
}

func (t *NotifyEventToPartner) activity(ctx context.Context, redriveEvent RedriveEvent) error {
	logger := zap.S().
		Named("notifyEventToPartnerActivity").
		With("webhook_id", redriveEvent.WebhookID, "partner_id", redriveEvent.PartnerID)
	ctx = logging.ContextWithLogger(ctx, logger)
	logger.Info("activity started")

	// Unmarshal event payload
	var e subscriber.Event
	if err := json.Unmarshal(redriveEvent.EventPayload, &e); err != nil {
		return fmt.Errorf("failed to unmarshal event payload: %w", err)
	}

	err := t.app.NotifyEventHandler.ExecuteWithLogID(ctx, e, redriveEvent.LogID, redriveEvent.RedriveCount, redriveEvent.CurrentTime)
	if err != nil {
		if isTerminalError(err) {
			return temporal.NewNonRetryableApplicationError(
				err.Error(), "TerminalError", err,
			)
		}
		handlerErr := fmt.Errorf("notify event to partner failed: %w", err)
		return handlerErr
	}
	return nil
}

func (t *NotifyEventToPartner) getWebhookActivity(ctx context.Context, webhookId string) (*webhook.Webhook, error) {
	logger := zap.S().With("webhook_id", webhookId)
	ctx = logging.ContextWithLogger(ctx, logger)
	logger.Info("getting webhook")

	wh, err := t.app.NotifyEventHandler.GetWebhookById(ctx, webhookId)
	if err != nil {
		return nil, fmt.Errorf("failed to get webhook: %w", err)
	}
	return wh, nil
}

func (t *NotifyEventToPartner) checkThrottlingActivity(ctx context.Context, partnerId string, rate int) (bool, error) {
	return t.app.RateLimiter.ShouldBlock(ctx, partnerId, rate)
}
