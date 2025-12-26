package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestGetWebhookById(t *testing.T) {
	const sampleWebhookId = "c84gk48qvh2l9t5dzp3f"
	const samplePartnerId = "cpaa1fg4uq1ne39r9p21"

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Mock Redis client (not connected, but for test)
	rdb := redis.NewClient(&redis.Options{})

	metadata := []byte(`{"name": "webhook name1", "events": ["subscriber.created", "subscriber.subscribed"], "post_url": "https://webhook.site/1e15250a-d7fb-4aef-a19a-0476c74ce911", "rate_limit_per_minute": 100, "secret_key": "secret"}`)
	mock.ExpectQuery("select id, partner_id, status, metadata, created_at, updated_at from webhook").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "partner_id", "status", "metadata", "created_at", "updated_at"}).
				AddRow(sampleWebhookId, samplePartnerId, "ACTIVE", metadata, time.Now(), time.Now()),
		)

	repo := NewRepository(db, rdb)
	w, err := repo.GetWebhookById(context.Background(), sampleWebhookId)

	assert.NoError(t, err)
	assert.Equal(t, w.Id, sampleWebhookId)
	assert.Equal(t, w.Status, webhook.StatusActive)
	assert.Equal(t, w.PartnerId, samplePartnerId)
	assert.Equal(t, w.Metadata.Events, []subscriber.EventName{subscriber.EventCreated, subscriber.EventSubscribed})

}
