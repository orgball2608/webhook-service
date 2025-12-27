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

	partnerIDKey    = temporal.NewSearchAttributeKeyKeyword(common.PartnerIDKey)
	webhookIDKey    = temporal.NewSearchAttributeKeyKeyword(common.WebhookIDKey)
	eventNameKey    = temporal.NewSearchAttributeKeyKeyword(common.EventNameKey)
	initialQueueKey = temporal.NewSearchAttributeKeyKeyword(common.InitialQueueKey)
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

func (t *NotifyEventToPartner) workflow(ctx workflow.Context, e subscriber.Event, initialQueue string) error {
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

	err = workflow.UpsertTypedSearchAttributes(ctx,
		partnerIDKey.ValueSet(wh.PartnerId),
		webhookIDKey.ValueSet(wh.Id),
		eventNameKey.ValueSet(string(e.EventName)),
		initialQueueKey.ValueSet(initialQueue),
	)
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

	// Phase 1: Initial attempt on selected queue
	aoPhase1 := workflow.ActivityOptions{
		TaskQueue:           initialQueue,
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}

	eventPayload, _ := json.Marshal(e)
	redriveEvent := RedriveEvent{
		LogID:              0,
		WebhookID:          e.WebhookId,
		PartnerID:          wh.PartnerId,
		EventPayload:       eventPayload,
		RateLimitPerMinute: wh.Metadata.RateLimitPerMinute,
		CurrentTime:        workflow.Now(ctx),
	}

	err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, aoPhase1), activityNotifyEventToPartner, redriveEvent).Get(ctx, nil)
	if err == nil {
		return nil
	}

	// Terminal errors (400, 401, 404) are not retried
	if isTerminalError(err) {
		return err
	}

	// Phase 2: Retry on Backlog queue with exponential backoff
	aoPhase2 := workflow.ActivityOptions{
		TaskQueue:           common.QueueBacklog,
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Minute,
			BackoffCoefficient:     2.0,
			MaximumInterval:        8 * time.Hour,
			MaximumAttempts:        14,
			NonRetryableErrorTypes: []string{"TerminalError"},
		},
	}

	return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, aoPhase2), activityNotifyEventToPartner, redriveEvent).Get(ctx, nil)
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
