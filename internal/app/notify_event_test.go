package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/stretchr/testify/assert"
)

func TestNotifyEventHandler_Execute(t *testing.T) {
	t.Run("success", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}
		w := &webhook.Webhook{
			Id:     sampleWebhookId,
			Status: webhook.StatusActive,
			Metadata: webhook.Metadata{
				Events: []subscriber.EventName{subscriber.EventSubscribed, subscriber.EventCreated},
			},
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(w, nil)
		webhookRepo.EXPECT().InsertWebhookLog(gomock.Any(), gomock.Any()).Return(nil)
		mockPartnerAdapter := NewMockPartnerAdapter(gomock.NewController(tt))
		mockPartnerAdapter.EXPECT().NotifyWebhookEvent(gomock.Any(), w, sampleEvent).Return(&webhook.WebhookResponse{}, nil)
		mockCircuitBreaker := NewMockCircuitBreaker(gomock.NewController(tt))
		mockCircuitBreaker.EXPECT().ShouldTrip(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, int64(0), int64(0), 0.0, nil)
		mockRateLimiter := NewMockRateLimiter(gomock.NewController(tt))
		mockRateLimiter.EXPECT().ShouldBlock(gomock.Any(), gomock.Any(), gomock.Any()).Return(false, nil)
		mockRegisterHandler := NewMockNotifyEventHandler(gomock.NewController(tt))

		cmdNotifyEventHandler := NewNotifyEventHandler(webhookRepo, mockPartnerAdapter, mockCircuitBreaker, mockRateLimiter, mockRegisterHandler)
		err := cmdNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.NoError(tt, err)
	})
}
