package app_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/stretchr/testify/assert"
)

func TestRegisterNotifyEventHandler_Execute(t *testing.T) {
	t.Run("success", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(&webhook.Webhook{
				Id:     sampleWebhookId,
				Status: webhook.StatusActive,
				Metadata: webhook.Metadata{
					Events:   []subscriber.EventName{subscriber.EventSubscribed, subscriber.EventCreated},
					Priority: common.PriorityHigh,
				},
			}, nil)
		webhookRepo.EXPECT().InsertWebhookLog(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		mockTemporalAdapter := NewMocktemporalAdapter(gomock.NewController(tt))
		mockTemporalAdapter.EXPECT().RegisterWorkflowNotifyEvent(gomock.Any(), sampleEvent).Return(nil)

		cmdRegisterNotifyEventHandler := NewRegisterNotifyEventHandler(mockTemporalAdapter, webhookRepo)
		err := cmdRegisterNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.NoError(tt, err)
	})

	t.Run("query webhook repo failed, return error for kafka consumer to retry", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(nil, fmt.Errorf("any db error"))
		mockTemporalAdapter := NewMocktemporalAdapter(gomock.NewController(tt))

		cmdRegisterNotifyEventHandler := NewRegisterNotifyEventHandler(mockTemporalAdapter, webhookRepo)
		err := cmdRegisterNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.Error(tt, err)
	})

	t.Run("webhook inactive, not register temporal workflow", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(&webhook.Webhook{
				Id:     sampleWebhookId,
				Status: webhook.StatusInactive,
				Metadata: webhook.Metadata{
					Events: []subscriber.EventName{subscriber.EventSubscribed, subscriber.EventCreated},
				},
			}, nil)
		mockTemporalAdapter := NewMocktemporalAdapter(gomock.NewController(tt))

		cmdRegisterNotifyEventHandler := NewRegisterNotifyEventHandler(mockTemporalAdapter, webhookRepo)
		err := cmdRegisterNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.NoError(tt, err)
	})

	t.Run("not register webhook with receiving event name, not register temporal workflow", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(&webhook.Webhook{
				Id:     sampleWebhookId,
				Status: webhook.StatusActive,
				Metadata: webhook.Metadata{
					Events: []subscriber.EventName{subscriber.EventUnsubscribed},
				},
			}, nil)
		mockTemporalAdapter := NewMocktemporalAdapter(gomock.NewController(tt))

		cmdRegisterNotifyEventHandler := NewRegisterNotifyEventHandler(mockTemporalAdapter, webhookRepo)
		err := cmdRegisterNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.NoError(tt, err)
	})

	t.Run("webhook not found, not register temporal workflow", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(nil, webhook.ErrRepositoryNotFound)
		mockTemporalAdapter := NewMocktemporalAdapter(gomock.NewController(tt))

		cmdRegisterNotifyEventHandler := NewRegisterNotifyEventHandler(mockTemporalAdapter, webhookRepo)
		err := cmdRegisterNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.NoError(tt, err)
	})

	t.Run("register temporal workflow failed, return error to consume kafka message again", func(tt *testing.T) {
		const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
		sampleEvent := subscriber.Event{
			EventName:  subscriber.EventSubscribed,
			EventTime:  time.Now(),
			Subscriber: subscriber.Subscriber{},
			Segment:    subscriber.Segment{},
			WebhookId:  sampleWebhookId,
		}

		webhookRepo := NewMockwebhookRepository(gomock.NewController(tt))
		webhookRepo.EXPECT().GetWebhookById(gomock.Any(), sampleWebhookId).
			Return(&webhook.Webhook{
				Id:     sampleWebhookId,
				Status: webhook.StatusActive,
				Metadata: webhook.Metadata{
					Events: []subscriber.EventName{subscriber.EventSubscribed, subscriber.EventCreated},
				},
			}, nil)
		mockTemporalAdapter := NewMocktemporalAdapter(gomock.NewController(tt))
		mockTemporalAdapter.EXPECT().RegisterWorkflowNotifyEvent(gomock.Any(), sampleEvent).Return(fmt.Errorf("any error"))

		cmdRegisterNotifyEventHandler := NewRegisterNotifyEventHandler(mockTemporalAdapter, webhookRepo)
		err := cmdRegisterNotifyEventHandler.Execute(context.Background(), sampleEvent)

		assert.Error(tt, err)
	})
}
