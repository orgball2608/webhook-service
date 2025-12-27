package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/patrickmn/go-cache"
)

var localPausedCache = cache.New(5*time.Minute, 10*time.Minute)

type RegisterNotifyEventHandler struct {
	temporalAdapter   temporalAdapter
	webhookRepository webhookRepository
}

func NewRegisterNotifyEventHandler(temporalAdapter temporalAdapter, webhookRepository webhookRepository) RegisterNotifyEventHandler {
	return RegisterNotifyEventHandler{
		temporalAdapter:   temporalAdapter,
		webhookRepository: webhookRepository,
	}
}

const (
	WorkflowNotifyEvent = "NotifyEventToPartner"
)

// Execute uses to verify valid event for webhook
// and trigger temporal workflow to notify to partner
func (h RegisterNotifyEventHandler) Execute(ctx context.Context, e subscriber.Event) error {
	if _, found := localPausedCache.Get("paused:" + e.WebhookId); found {
		return nil // Skip immediately
	}

	w, err := h.webhookRepository.GetWebhookById(ctx, e.WebhookId)
	if errors.Is(err, webhook.ErrRepositoryNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get event webhook: %w", err)
	}
	if w.Status == webhook.StatusPaused {
		localPausedCache.Set("paused:"+w.Id, true, 10*time.Second)
		return nil
	}
	if w.Status != webhook.StatusActive {
		return nil
	}
	if !slices.Contains(w.Metadata.Events, e.EventName) {
		return nil // notify if not registered for event
	}

	err = h.temporalAdapter.RegisterWorkflowNotifyEvent(ctx, e)
	if err != nil {
		return fmt.Errorf("temporal adapter failed to trigger notify webhook: %w", err)
	}
	return nil
}

func (h RegisterNotifyEventHandler) ExecuteWithLogID(ctx context.Context, e subscriber.Event, logID int64, redriveCount int, currentTime time.Time) error {
	// Not used for register
	return fmt.Errorf("ExecuteWithLogID not implemented for RegisterNotifyEventHandler")
}
