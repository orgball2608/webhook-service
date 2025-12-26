package temporal_workflow

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/adapters/repository/webhook"
	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const defaultPageSize = 100

type RedriveEvent struct {
	LogID              int64
	WebhookID          string
	PartnerID          string
	EventPayload       []byte
	RateLimitPerMinute int
	RedriveCount       int // số lần redrive
	CurrentTime        time.Time
}

type RedriveActivities struct {
	repo webhook.Repository
}

func NewRedriveActivities(repo webhook.Repository) *RedriveActivities {
	return &RedriveActivities{repo: repo}
}

func (a *RedriveActivities) ScanFailedPartnersActivity(ctx context.Context) ([]string, error) {
	return a.repo.ScanFailedPartners(ctx)
}

func (a *RedriveActivities) ScanDLQActivity(ctx context.Context, partnerID string, pageSize int) ([]RedriveEvent, error) {
	rows, err := a.repo.ScanDLQForRedrive(ctx, partnerID, pageSize)
	if err != nil {
		return nil, err
	}
	var result []RedriveEvent

	// Batch fetch webhooks to avoid N+1 query
	webhookIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		webhookIDs = append(webhookIDs, row.WebhookID)
	}
	webhookMap, _ := a.repo.GetWebhooksByIDs(ctx, webhookIDs)
	for _, row := range rows {
		var event subscriber.Event
		if err := json.Unmarshal(row.EventPayload, &event); err != nil {
			log.Printf("[PoisonPill] Unmarshal event_payload failed: %v", err)
			_ = a.repo.UpdateWebhookLogRedriveStatus(ctx, row.ID, true, "permanently_failed: invalid payload", nil)
			continue
		}
		wh := webhookMap[row.WebhookID]
		rate := 20
		if wh != nil && wh.Metadata.RateLimitPerMinute > 0 {
			rate = wh.Metadata.RateLimitPerMinute
		}
		result = append(result, RedriveEvent{
			LogID:              row.ID,
			WebhookID:          row.WebhookID,
			PartnerID:          row.PartnerID,
			EventPayload:       row.EventPayload,
			RateLimitPerMinute: rate,
			RedriveCount:       int(row.RedriveCount),
		})
	}
	return result, nil
}

func CronRedriveWorkflow(ctx workflow.Context) error {
	ctxActivity := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 30,
	})

	var partners []string
	err := workflow.ExecuteActivity(ctxActivity, "ScanFailedPartnersActivity").Get(ctx, &partners)
	if err != nil {
		return err
	}
	for _, partnerID := range partners {
		_ = workflow.ExecuteChildWorkflow(ctx, RedrivePartnerEventsWorkflow, partnerID)
	}
	return nil
}

func RedrivePartnerEventsWorkflow(ctx workflow.Context, partnerID string) error {
	ctxActivity := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 30,
	})

	pageSize := defaultPageSize
	for {
		var events []RedriveEvent
		err := workflow.ExecuteActivity(ctxActivity, "ScanDLQActivity", partnerID, pageSize).Get(ctx, &events)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			if err := workflow.ExecuteActivity(ctxActivity, "ActivityNotifyEventToPartner", event).Get(ctx, nil); err != nil {
				// Nếu lỗi 429 hoặc 5xx, tính toán next_redrive_at cho exponential backoff
				var nextRedriveAt *time.Time
				if is429Or5xx(err) {
					baseDelay := 5 * time.Minute
					backoff := baseDelay * time.Duration(1<<event.RedriveCount)
					t := workflow.Now(ctx).Add(backoff)
					nextRedriveAt = &t
				}
				_ = workflow.ExecuteActivity(ctxActivity, "ActivityUpdateLogRedriveStatus", UpdateLogParams{
					LogID:         event.LogID,
					IsResolved:    false,
					ErrorMsg:      err.Error(),
					NextRedriveAt: nextRedriveAt,
				}).Get(ctx, nil)
				continue
			}
			rate := event.RateLimitPerMinute
			if rate <= 0 {
				rate = 20
			}
			// Exponential backoff: sleep = 1m * 2^redriveCount
			backoff := time.Minute * time.Duration(1<<event.RedriveCount)
			if err := workflow.Sleep(ctx, backoff); err != nil {
				return err
			}
		}
		if len(events) == pageSize {
			return workflow.NewContinueAsNewError(ctx, RedrivePartnerEventsWorkflow, partnerID)
		}
	}
	return nil
}

func RegisterCronRedriveWorkflow(temporalWorker worker.Worker, redrive *RedriveActivities) {
	temporalWorker.RegisterWorkflow(CronRedriveWorkflow)
	temporalWorker.RegisterWorkflow(RedrivePartnerEventsWorkflow)
	temporalWorker.RegisterActivity(redrive.ScanFailedPartnersActivity)
	temporalWorker.RegisterActivity(redrive.ScanDLQActivity)
	temporalWorker.RegisterActivity(redrive.ActivityUpdateLogRedriveStatus)
}

type UpdateLogParams struct {
	LogID         int64
	IsResolved    bool
	ErrorMsg      string
	NextRedriveAt *time.Time
}

func (a *RedriveActivities) ActivityUpdateLogRedriveStatus(ctx context.Context, params UpdateLogParams) error {
	return a.repo.UpdateWebhookLogRedriveStatus(ctx, params.LogID, params.IsResolved, params.ErrorMsg, params.NextRedriveAt)
}

func is429Or5xx(err error) bool {
	var appErr *app.WebhookResponseError
	if errors.As(err, &appErr) {
		if appErr.StatusCode == 429 || appErr.StatusCode >= 500 {
			return true
		}
	}
	return false
}
