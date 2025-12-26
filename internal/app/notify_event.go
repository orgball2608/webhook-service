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
	webhookRepository webhookRepository
	circuitBreaker    CircuitBreaker
	rateLimiter       RateLimiter
	RegisterHandler   NotifyEventHandler
}

func NewNotifyEventHandler(webhookRepository webhookRepository, partnerAdapter PartnerAdapter, circuitBreaker CircuitBreaker, rateLimiter RateLimiter, registerHandler NotifyEventHandler) NotifyEventHandler {
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

	// 1. Kiểm tra Circuit Breaker
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
	if err == nil && resp != nil && resp.StatusCode < 500 {
		h.handleSuccess(ctx, w.Id)
		return h.logResult(ctx, w, e, resp, nil)
	}

	if resp != nil && resp.StatusCode == 429 {
		h.handleFailure(ctx, w.Id)
		return h.logResult(ctx, w, e, resp, fmt.Errorf("PARTNER_RATE_LIMIT"))
	}

	// 5xx or Network Timeout: Fallback to Temporal
	return h.fallbackToTemporal(ctx, e)
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
		// Auto-disable logic
		streak, streakErr := h.webhookRepository.IncrFailStreak(ctx, w.Id)
		if streakErr == nil && streak > 100 {
			// Auto-disable webhook
			if updateErr := h.webhookRepository.UpdateWebhookStatus(ctx, w.Id, webhook.StatusPaused); updateErr != nil {
				fmt.Printf("failed to auto-disable webhook %s: %v\n", w.Id, updateErr)
			} else {
				fmt.Printf("auto-disabled webhook %s due to high fail streak\n", w.Id)
			}
		}
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
	shouldTrip, _, _, _, _ := h.circuitBreaker.ShouldTrip(ctx, webhookID, 90, 20, 300) // Default values
	return shouldTrip
}

func (h notifyEventHandlerImpl) handleSuccess(ctx context.Context, webhookID string) {
	// Reset fail streak on success
	// TODO: Implement reset logic if needed
}

func (h notifyEventHandlerImpl) handleFailure(ctx context.Context, webhookID string) {
	// Already handled in logResult
}

func (h notifyEventHandlerImpl) logResult(ctx context.Context, w *webhook.Webhook, evt subscriber.Event, resp *webhook.WebhookResponse, err error) error {
	eventPayload, _ := json.Marshal(evt)
	var statusCode *int
	var body, errMsg *string
	if resp != nil {
		statusCode = &resp.StatusCode
		body = &resp.Body
		errMsg = &resp.Error
	}
	if err != nil && errMsg == nil {
		errStr := err.Error()
		errMsg = &errStr
	}
	status := "SUCCESS"
	if err != nil {
		status = "FAILED"
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

func (h notifyEventHandlerImpl) fallbackToTemporal(ctx context.Context, evt subscriber.Event) error {
	return h.RegisterHandler.Execute(ctx, evt)
}
