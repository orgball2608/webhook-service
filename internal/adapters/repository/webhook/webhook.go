package webhook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/minhvuongrbs/webhook-service/internal/adapters/repository/sqlc/da_generated"
	"github.com/minhvuongrbs/webhook-service/internal/app"
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

// Ensure Repository implements app.WebhookRepository
var _ app.WebhookRepository = (*Repository)(nil)

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
	// Use bucketed counters: per minute
	now := time.Now().Unix()
	minuteBucket := now / 60
	key := fmt.Sprintf("%s%s:%d", common.RedisKeyFailRate, webhookID, minuteBucket)
	count, err := r.redis.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// Set TTL for the bucket (e.g., 2 hours to keep recent data)
	r.redis.Expire(ctx, key, 2*time.Hour)
	return count, nil
}

func (r Repository) GetFailRate(ctx context.Context, webhookID string) (float64, error) {
	now := time.Now().Unix()
	// Sum last 60 minutes
	total := int64(0)
	for i := int64(0); i < 60; i++ {
		minuteBucket := (now / 60) - i
		key := fmt.Sprintf("%s%s:%d", common.RedisKeyFailRate, webhookID, minuteBucket)
		count, err := r.redis.Get(ctx, key).Int64()
		if err == nil {
			total += count
		}
	}
	// For success
	successTotal := int64(0)
	for i := int64(0); i < 60; i++ {
		minuteBucket := (now / 60) - i
		key := fmt.Sprintf("%s%s:%d", common.RedisKeySuccessRate, webhookID, minuteBucket)
		count, err := r.redis.Get(ctx, key).Int64()
		if err == nil {
			successTotal += count
		}
	}
	grandTotal := total + successTotal
	if grandTotal == 0 {
		return 0, nil
	}
	return float64(total) / float64(grandTotal) * 100, nil
}

func (r Repository) ResetFailRate(ctx context.Context, webhookID string) error {
	failKey := common.RedisKeyFailRate + webhookID
	successKey := common.RedisKeySuccessRate + webhookID
	r.redis.Del(ctx, failKey)
	r.redis.Del(ctx, successKey)
	return nil
}

func (r Repository) IncrSuccessRate(ctx context.Context, webhookID string) error {
	// Use bucketed counters: per minute
	now := time.Now().Unix()
	minuteBucket := now / 60
	key := fmt.Sprintf("%s%s:%d", common.RedisKeySuccessRate, webhookID, minuteBucket)
	_, err := r.redis.Incr(ctx, key).Result()
	if err != nil {
		return err
	}
	// Set TTL for the bucket (e.g., 2 hours to keep recent data)
	r.redis.Expire(ctx, key, 2*time.Hour)
	return nil
}

func (r Repository) Incr4xxRate(ctx context.Context, webhookID string) (int64, error) {
	// Use bucketed counters: per minute
	now := time.Now().Unix()
	minuteBucket := now / 60
	key := fmt.Sprintf("%s%s:%d", common.RedisKey4xxRate, webhookID, minuteBucket)
	count, err := r.redis.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// Set TTL for the bucket (e.g., 2 hours to keep recent data)
	r.redis.Expire(ctx, key, 2*time.Hour)
	return count, nil
}

func (r Repository) Get4xxRate(ctx context.Context, webhookID string) (float64, error) {
	now := time.Now().Unix()
	// Sum last 1440 minutes (24 hours)
	total := int64(0)
	for i := int64(0); i < 1440; i++ {
		minuteBucket := (now / 60) - i
		key := fmt.Sprintf("%s%s:%d", common.RedisKey4xxRate, webhookID, minuteBucket)
		count, err := r.redis.Get(ctx, key).Int64()
		if err == nil {
			total += count
		}
	}
	// For total requests in 24 hours
	successTotal := int64(0)
	for i := int64(0); i < 1440; i++ {
		minuteBucket := (now / 60) - i
		key := fmt.Sprintf("%s%s:%d", common.RedisKeySuccessRate, webhookID, minuteBucket)
		count, err := r.redis.Get(ctx, key).Int64()
		if err == nil {
			successTotal += count
		}
	}
	failTotal := int64(0)
	for i := int64(0); i < 1440; i++ {
		minuteBucket := (now / 60) - i
		key := fmt.Sprintf("%s%s:%d", common.RedisKeyFailRate, webhookID, minuteBucket)
		count, err := r.redis.Get(ctx, key).Int64()
		if err == nil {
			failTotal += count
		}
	}
	grandTotal := total + successTotal + failTotal
	if grandTotal == 0 {
		return 0, nil
	}
	return float64(total) / float64(grandTotal) * 100, nil
}

func (r Repository) IncrStats(ctx context.Context, webhookID string, isSuccess bool) {
	bucket := time.Now().Unix() / 600 // 10-minute buckets
	typeStr := "success"
	if !isSuccess {
		typeStr = "fail"
	}
	key := fmt.Sprintf("webhook:stats:%s:%s:%d", webhookID, typeStr, bucket)
	r.redis.Incr(ctx, key)
	r.redis.Expire(ctx, key, 7*time.Hour)
}

func (r Repository) GetActiveWebhooks(ctx context.Context) ([]*webhook.Webhook, error) {
	return r.GetActiveWebhooksPaginated(ctx, 0, 500)
}

// GetActiveWebhooksPaginated returns a page of active webhooks (for batch processing)
func (r Repository) GetActiveWebhooksPaginated(ctx context.Context, offset, limit int) ([]*webhook.Webhook, error) {
	q := da_generated.New(r.db)
	rows, err := q.GetActiveWebhooksPaginated(ctx, &da_generated.GetActiveWebhooksPaginatedParams{
		Limit:  int32(limit),
		Offset: int32(offset),
	})
	if err != nil {
		return nil, err
	}
	result := make([]*webhook.Webhook, 0, len(rows))
	for _, w := range rows {
		var md webhook.Metadata
		_ = json.Unmarshal(w.Metadata, &md)
		result = append(result, &webhook.Webhook{
			Id:        w.ID,
			Status:    toEntityStatus(w.Status),
			PartnerId: w.PartnerID,
			Metadata:  md,
			CreatedAt: w.CreatedAt,
			UpdatedAt: w.UpdatedAt,
		})
	}
	return result, nil
}

func (r Repository) CalculateSuccessRate(ctx context.Context, webhookID string) (float64, int64, error) {
	now := time.Now().Unix()
	var (
		successKeys []string
		failKeys    []string
	)
	for i := int64(0); i < 36; i++ {
		bucket := (now / 600) - i
		successKeys = append(successKeys, fmt.Sprintf("webhook:stats:%s:success:%d", webhookID, bucket))
		failKeys = append(failKeys, fmt.Sprintf("webhook:stats:%s:fail:%d", webhookID, bucket))
	}
	// MGet for all success and fail keys
	successVals, _ := r.redis.MGet(ctx, successKeys...).Result()
	failVals, _ := r.redis.MGet(ctx, failKeys...).Result()

	successTotal := int64(0)
	failTotal := int64(0)
	for _, v := range successVals {
		if v == nil {
			continue
		}
		if n, err := toInt64(v); err == nil {
			successTotal += n
		}
	}
	for _, v := range failVals {
		if v == nil {
			continue
		}
		if n, err := toInt64(v); err == nil {
			failTotal += n
		}
	}
	total := successTotal + failTotal
	if total == 0 {
		return 0, 0, nil
	}
	rate := float64(successTotal) / float64(total) * 100
	return rate, total, nil
}

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case string:
		return parseInt(val)
	case []byte:
		return parseInt(string(val))
	}
	return 0, fmt.Errorf("cannot convert %v to int64", v)
}

func parseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func (r Repository) FetchWebhooksByIDs(ctx context.Context, ids []string) ([]*webhook.Webhook, error) {
	if len(ids) == 0 {
		return []*webhook.Webhook{}, nil
	}
	q := da_generated.New(r.db)
	rows, err := q.GetWebhooksByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	result := make([]*webhook.Webhook, 0, len(rows))
	for _, w := range rows {
		var md webhook.Metadata
		_ = json.Unmarshal(w.Metadata, &md)
		result = append(result, &webhook.Webhook{
			Id:        w.ID,
			Status:    toEntityStatus(w.Status),
			PartnerId: w.PartnerID,
			Metadata:  md,
			CreatedAt: w.CreatedAt,
			UpdatedAt: w.UpdatedAt,
		})
	}
	return result, nil
}
