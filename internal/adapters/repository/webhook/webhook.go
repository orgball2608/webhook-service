package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/adapters/repository/sqlc/da_generated"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
	"github.com/redis/go-redis/v9"
	"github.com/samber/lo"

	"github.com/minhvuongrbs/webhook-service/internal/common"
)

type Repository struct {
	db    *sql.DB
	redis *redis.Client
}

func NewRepository(db *sql.DB, redis *redis.Client) Repository {
	return Repository{db: db, redis: redis}
}

func (r Repository) GetWebhookById(ctx context.Context, webhookId string) (*webhook.Webhook, error) {
	// Try cache first
	cacheKey := "webhook:" + webhookId
	cached, err := r.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		var w webhook.Webhook
		if err := json.Unmarshal([]byte(cached), &w); err == nil {
			return &w, nil
		}
		// If unmarshal fails, fall back to DB
	}

	// Cache miss or error, query DB
	q := da_generated.New(r.db)
	w, err := q.GetWebhookById(ctx, webhookId)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, webhook.ErrRepositoryNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query db error: %w", err)
	}

	var md webhook.Metadata
	err = json.Unmarshal(w.Metadata, &md)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal metadata error: %w", err)
	}
	// Debug log
	fmt.Printf("DEBUG: webhook %s metadata: %+v\n", w.ID, md)
	// Debug: print metadata after unmarshal
	fmt.Printf("DEBUG: webhook %s metadata after unmarshal: %+v\n", w.ID, md)

	wh := &webhook.Webhook{
		Id:        w.ID,
		Status:    toEntityStatus(w.Status),
		PartnerId: w.PartnerID,
		Metadata:  md,
		CreatedAt: w.CreatedAt,
		UpdatedAt: w.UpdatedAt,
	}

	// Cache the result
	if cachedData, err := json.Marshal(wh); err == nil {
		_ = r.redis.Set(ctx, cacheKey, cachedData, time.Hour).Err() // TTL 1 hour
	}

	return wh, nil
}

func toEntityStatus(s string) webhook.Status {
	switch strings.ToUpper(s) {
	case "ACTIVE":
		return webhook.StatusActive
	case "INACTIVE":
		return webhook.StatusInactive
	case "PAUSED":
		return webhook.StatusPaused
	}
	return webhook.StatusInactive
}

func (r Repository) InsertWebhookLog(ctx context.Context, log *webhook.Log) error {
	q := da_generated.New(r.db)
	params := &da_generated.InsertWebhookLogParams{
		WebhookID:      log.WebhookID,
		PartnerID:      log.PartnerID,
		EventPayload:   []byte(log.EventPayload),
		ResponseStatus: sql.NullInt32{Int32: int32(lo.FromPtr(log.ResponseStatus)), Valid: log.ResponseStatus != nil},
		ResponseBody:   sql.NullString{String: lo.FromPtr(log.ResponseBody), Valid: log.ResponseBody != nil},
		ErrorMessage:   sql.NullString{String: lo.FromPtr(log.ErrorMessage), Valid: log.ErrorMessage != nil},
		Status:         sql.NullString{String: lo.FromPtr(log.Status), Valid: log.Status != nil},
	}
	return q.InsertWebhookLog(ctx, params)
}

func (r Repository) ScanDLQForRedrive(ctx context.Context, _ string, limit int) ([]da_generated.ScanDLQForRedriveRow, error) {
	q := da_generated.New(r.db)
	rows, err := q.ScanDLQForRedrive(ctx, int32(limit))
	if err != nil {
		return nil, err
	}
	result := make([]da_generated.ScanDLQForRedriveRow, 0, len(rows))
	for _, row := range rows {
		if row != nil {
			result = append(result, *row)
		}
	}
	return result, nil
}

func (r Repository) UpdateWebhookLogRedriveStatus(ctx context.Context, id int64, isResolved bool, errorMsg string, nextRedriveAt *time.Time) error {
	q := da_generated.New(r.db)
	params := &da_generated.UpdateWebhookLogRedriveStatusParams{
		ID:         id,
		IsResolved: isResolved,
		ErrorMessage: sql.NullString{
			String: errorMsg,
			Valid:  errorMsg != "",
		},
		NextRedriveAt: sql.NullTime{},
	}
	if nextRedriveAt != nil {
		params.NextRedriveAt = sql.NullTime{Time: *nextRedriveAt, Valid: true}
	}
	return q.UpdateWebhookLogRedriveStatus(ctx, params)
}

func (r Repository) ScanFailedPartners(ctx context.Context) ([]string, error) {
	q := da_generated.New(r.db)
	rows, err := q.ScanFailedPartners(ctx)
	if err != nil {
		return nil, err
	}
	partners := make([]string, 0, len(rows))
	for _, row := range rows {
		partners = append(partners, row)
	}
	return partners, nil
}

func (r Repository) UpdateWebhookStatus(ctx context.Context, webhookId string, status webhook.Status) error {
	q := da_generated.New(r.db)
	_, err := q.UpdateWebhook(ctx, &da_generated.UpdateWebhookParams{
		ID:     webhookId,
		Status: string(status),
	})
	if err == nil {
		cacheKey := "webhook:" + webhookId
		_ = r.redis.Del(ctx, cacheKey).Err()
	}
	return err
}

func (r Repository) GetWebhooksByIDs(ctx context.Context, webhookIDs []string) (map[string]*webhook.Webhook, error) {
	result := make(map[string]*webhook.Webhook)
	if len(webhookIDs) == 0 {
		return result, nil
	}
	q := da_generated.New(r.db)
	rows, err := q.GetWebhooksByIDs(ctx, webhookIDs)
	if err != nil {
		return nil, err
	}
	for _, w := range rows {
		var md webhook.Metadata
		_ = json.Unmarshal(w.Metadata, &md)
		result[w.ID] = &webhook.Webhook{
			Id:        w.ID,
			Status:    toEntityStatus(w.Status),
			PartnerId: w.PartnerID,
			Metadata:  md,
			CreatedAt: w.CreatedAt,
			UpdatedAt: w.UpdatedAt,
		}
	}
	return result, nil
}

func (r Repository) UpdateWebhookLogStatus(ctx context.Context, id int64, status string) error {
	q := da_generated.New(r.db)
	err := q.UpdateWebhookLogStatus(ctx, &da_generated.UpdateWebhookLogStatusParams{
		ID:     id,
		Status: sql.NullString{String: status, Valid: true},
	})
	return err
}

func (r Repository) IncrFailRate(ctx context.Context, webhookID string) (int64, error) {
	key := common.RedisKeyFailRate + webhookID
	now := float64(time.Now().Unix())
	// Add timestamp to sorted set
	err := r.redis.ZAdd(ctx, key, redis.Z{Score: now, Member: now}).Err()
	if err != nil {
		return 0, err
	}
	// Remove old entries (older than 1 hour)
	min := fmt.Sprintf("%f", float64(time.Now().Add(-time.Hour).Unix()))
	r.redis.ZRemRangeByScore(ctx, key, "-inf", min)
	// Get count
	count, err := r.redis.ZCard(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (r Repository) GetFailRate(ctx context.Context, webhookID string) (float64, error) {
	failKey := common.RedisKeyFailRate + webhookID
	successKey := common.RedisKeySuccessRate + webhookID
	min := fmt.Sprintf("%f", float64(time.Now().Add(-time.Hour).Unix()))

	// Clean old entries
	r.redis.ZRemRangeByScore(ctx, failKey, "-inf", min)
	r.redis.ZRemRangeByScore(ctx, successKey, "-inf", min)

	failCount, err := r.redis.ZCard(ctx, failKey).Result()
	if err != nil {
		return 0, err
	}
	successCount, err := r.redis.ZCard(ctx, successKey).Result()
	if err != nil {
		return 0, err
	}
	total := failCount + successCount
	if total == 0 {
		return 0, nil
	}
	return float64(failCount) / float64(total) * 100, nil
}

func (r Repository) ResetFailRate(ctx context.Context, webhookID string) error {
	failKey := common.RedisKeyFailRate + webhookID
	successKey := common.RedisKeySuccessRate + webhookID
	r.redis.Del(ctx, failKey)
	r.redis.Del(ctx, successKey)
	return nil
}

func (r Repository) IncrSuccessRate(ctx context.Context, webhookID string) error {
	key := common.RedisKeySuccessRate + webhookID
	now := float64(time.Now().Unix())
	// Add timestamp to sorted set
	err := r.redis.ZAdd(ctx, key, redis.Z{Score: now, Member: now}).Err()
	if err != nil {
		return err
	}
	// Remove old entries (older than 1 hour)
	min := fmt.Sprintf("%f", float64(time.Now().Add(-time.Hour).Unix()))
	r.redis.ZRemRangeByScore(ctx, key, "-inf", min)
	return nil
}
