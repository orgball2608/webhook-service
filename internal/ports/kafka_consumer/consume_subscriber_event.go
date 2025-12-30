package kafka_consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/pkg/kafka"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
)

type ConsumeSubscriberEvent struct {
	rateLimiter    app.RateLimiter
	App            app.App
	partnerAdapter app.PartnerAdapter
}

func NewConsumeSubscriberEvent(app app.App, partnerAdapter app.PartnerAdapter, rateLimit app.RateLimiter) ConsumeSubscriberEvent {
	return ConsumeSubscriberEvent{
		App:            app,
		partnerAdapter: partnerAdapter,
		rateLimiter:    rateLimit,
	}
}

func (c ConsumeSubscriberEvent) Handle(ctx context.Context, message *kafka.ConsumerMessage) error {
	l := logging.FromContext(ctx)

	const maxPayloadSize = 1 * 1024 * 1024 // 1MB
	if len(message.Payload) > maxPayloadSize {
		l.Errorw("payload too large, dropping event to prevent OOM", "size", len(message.Payload), "max_allowed", maxPayloadSize)
		return nil
	}

	var evt subscriber.Event
	if err := json.Unmarshal(message.Payload, &evt); err != nil {
		l.Warnw("cannot unmarshal subscriber event", "error", err, "payload", string(message.Payload))
		return nil
	}

	switch evt.EventName {
	case subscriber.EventCreated, subscriber.EventSubscribed, subscriber.EventUnsubscribed:
		l.Infow("starting handle event", "event_name", evt.EventName, "webhook_id", evt.WebhookId)

		// Delegate to App Layer for all logic
		return c.App.NotifyEventHandler.Execute(ctx, evt)
	default:
		return fmt.Errorf("unknown event type: %s", evt.EventName)
	}
}
