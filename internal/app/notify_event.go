package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
)

type notifyEventHandlerImpl struct {
	partnerAdapter    PartnerAdapter
	webhookRepository WebhookRepository
	circuitBreaker    CircuitBreaker
	rateLimiter       RateLimiter
	RegisterHandler   NotifyEventHandler
}

func NewNotifyEventHandler(webhookRepository WebhookRepository, partnerAdapter PartnerAdapter, circuitBreaker CircuitBreaker, rateLimiter RateLimiter, registerHandler NotifyEventHandler) NotifyEventHandler {
	return notifyEventHandlerImpl{
		webhookRepository: webhookRepository,
		partnerAdapter:    partnerAdapter,
		circuitBreaker:    circuitBreaker,
		rateLimiter:       rateLimiter,
		RegisterHandler:   registerHandler,
	}
}

func (h notifyEventHandlerImpl) Execute(ctx context.Context, e subscriber.Event) error {
	w, err := h.webhookRepository.GetWebhookById(ctx, e.WebhookId)
	if err != nil {
		return h.fallbackToTemporal(ctx, e)
	}

	// Check circuit breaker
	if h.isCircuitOpen(ctx, w.Id) {
		return h.fallbackToTemporal(ctx, e)
	}

	// 2. Kiểm tra Rate Limit nội bộ
	isBlocked, _ := h.rateLimiter.ShouldBlock(ctx, w.PartnerId, w.Metadata.RateLimitPerMinute)
	if isBlocked {
		return h.fallbackToTemporal(ctx, e)
	}

	// 3. Eager Send
	resp, err := h.partnerAdapter.NotifyWebhookEvent(ctx, w, e)
	var webhookResp *webhook.WebhookResponse
	if resp != nil {
		webhookResp = resp
	}

	status := "SUCCESS"
	if err != nil {
		status = "FAILED"
		// Record failure stat
		h.webhookRepository.IncrStats(ctx, w.Id, false)
	} else {
		// Record success stat
		h.webhookRepository.IncrStats(ctx, w.Id, true)
	}

	// Log the webhook notification
	eventPayload, _ := json.Marshal(e)
	var statusCode *int
	var body, errMsg *string
	if webhookResp != nil {
		statusCode = &webhookResp.StatusCode
		body = &webhookResp.Body
		errMsg = &webhookResp.Error
	}

	log := &webhook.Log{
		WebhookID:      w.Id,
		PartnerID:      w.PartnerId,
		EventPayload:   string(eventPayload),
		ResponseStatus: statusCode,
		ResponseBody:   body,
		ErrorMessage:   errMsg,
		Status:         &status,
	}

	return h.webhookRepository.InsertWebhookLog(ctx, log)
}

func (h notifyEventHandlerImpl) ExecuteWithLogID(ctx context.Context, e subscriber.Event, logID int64, redriveCount int, currentTime time.Time) error {
	w, err := h.webhookRepository.GetWebhookById(ctx, e.WebhookId)
	if err != nil {
		return fmt.Errorf("get event webhook: %w", err)
	}

	cbCfg := w.Metadata
	shouldTrip, _, _, _, _ := h.circuitBreaker.ShouldTrip(ctx, w.Id, cbCfg.ErrorThresholdPercentage, cbCfg.MinRequestsToTrip, cbCfg.EvaluationWindowSeconds)
	if shouldTrip {
		return fmt.Errorf("circuit breaker tripped for webhook %s", w.Id)
	}

	resp, err := h.partnerAdapter.NotifyWebhookEvent(ctx, w, e)
	var webhookResp *webhook.WebhookResponse
	if resp != nil {
		webhookResp = resp
	}

	status := "SUCCESS"
	if err != nil {
		status = "FAILED"
		// Record failure stat
		h.webhookRepository.IncrStats(ctx, w.Id, false)
	} else {
		// Record success stat
		h.webhookRepository.IncrStats(ctx, w.Id, true)
	}

	// Log the webhook notification
	eventPayload, _ := json.Marshal(e)
	var statusCode *int
	var body, errMsg *string
	if webhookResp != nil {
		statusCode = &webhookResp.StatusCode
		body = &webhookResp.Body
		errMsg = &webhookResp.Error
	}

	log := &webhook.Log{
		WebhookID:      w.Id,
		PartnerID:      w.PartnerId,
		EventPayload:   string(eventPayload),
		ResponseStatus: statusCode,
		ResponseBody:   body,
		ErrorMessage:   errMsg,
		Status:         &status,
	}

	if logID > 0 {
		// Update existing log for redrive
		isResolved := true
		errorMsg := ""
		var nextRedriveAt *time.Time
		if webhookResp != nil && webhookResp.StatusCode >= 500 {
			isResolved = false
			errorMsg = webhookResp.Error
			// Exponential backoff for redrive
			baseDelay := 5 * time.Minute
			backoff := baseDelay * time.Duration(1<<redriveCount)
			t := currentTime.Add(backoff)
			nextRedriveAt = &t
		}
		if err := h.webhookRepository.UpdateWebhookLogRedriveStatus(ctx, logID, isResolved, errorMsg, nextRedriveAt); err != nil {
			fmt.Printf("failed to update webhook log: %v\n", err)
		}
	} else {
		// Insert new log for first attempt
		if err := h.webhookRepository.InsertWebhookLog(ctx, log); err != nil {
			fmt.Printf("failed to insert webhook log: %v\n", err)
		}
	}
	return nil
}

func (h notifyEventHandlerImpl) GetWebhookById(ctx context.Context, webhookId string) (*webhook.Webhook, error) {
	return h.webhookRepository.GetWebhookById(ctx, webhookId)
}

func (h notifyEventHandlerImpl) InsertWebhookLog(ctx context.Context, log *webhook.Log) error {
	return h.webhookRepository.InsertWebhookLog(ctx, log)
}

func (h notifyEventHandlerImpl) isCircuitOpen(ctx context.Context, webhookID string) bool {
	w, err := h.webhookRepository.GetWebhookById(ctx, webhookID)
	if err != nil || w == nil {
		return false // fallback: do not trip CB if cannot get config
	}
	cbCfg := w.Metadata
	shouldTrip, _, _, _, _ := h.circuitBreaker.ShouldTrip(ctx, webhookID, cbCfg.ErrorThresholdPercentage, cbCfg.MinRequestsToTrip, cbCfg.EvaluationWindowSeconds)
	return shouldTrip
}

func (h notifyEventHandlerImpl) handleSuccess(ctx context.Context, webhookID string) {
	if err := h.webhookRepository.IncrSuccessRate(ctx, webhookID); err != nil {
		fmt.Printf("failed to increment success rate for webhook %s: %v\n", webhookID, err)
	}
}

func (h notifyEventHandlerImpl) handleFailure(ctx context.Context, webhookID string) {
	if _, err := h.webhookRepository.IncrFailRate(ctx, webhookID); err != nil {
		fmt.Printf("failed to increment fail rate for webhook %s: %v\n", webhookID, err)
	}
}

func (h notifyEventHandlerImpl) fallbackToTemporal(ctx context.Context, evt subscriber.Event) error {
	return h.RegisterHandler.Execute(ctx, evt)
}

func (h notifyEventHandlerImpl) GetActiveWebhooks(ctx context.Context) ([]*webhook.Webhook, error) {
	return h.webhookRepository.GetActiveWebhooks(ctx)
}

func (h notifyEventHandlerImpl) GetActiveWebhooksPaginated(ctx context.Context, offset, limit int) ([]*webhook.Webhook, error) {
	return h.webhookRepository.GetActiveWebhooksPaginated(ctx, offset, limit)
}

func (h notifyEventHandlerImpl) CalculateSuccessRate(ctx context.Context, webhookID string) (float64, int64, error) {
	return h.webhookRepository.CalculateSuccessRate(ctx, webhookID)
}

func (h notifyEventHandlerImpl) DisableWebhook(ctx context.Context, webhookID string) error {
	return h.webhookRepository.UpdateWebhookStatus(ctx, webhookID, webhook.StatusInactive)
}

func (h notifyEventHandlerImpl) FetchWebhooksByIDs(ctx context.Context, ids []string) ([]*webhook.Webhook, error) {
	return h.webhookRepository.FetchWebhooksByIDs(ctx, ids)
}
