package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

type RedisRateLimiter struct {
	Client  *redis.Client
	Limiter *redis_rate.Limiter
}

func NewRedisRateLimiter(client *redis.Client) *RedisRateLimiter {
	return &RedisRateLimiter{
		Client:  client,
		Limiter: redis_rate.NewLimiter(client),
	}
}

func (r *RedisRateLimiter) ShouldBlock(ctx context.Context, partnerID string, rate int) (bool, error) {
	res, err := r.Limiter.Allow(ctx, partnerID, redis_rate.PerMinute(rate))
	if err != nil {
		return false, err
	}
	if res.Allowed == 0 {
		return true, nil
	}
	return false, nil
}

// Circuit Breaker Manager using sony/gobreaker

type CircuitBreakerManager struct {
	breakers map[string]*gobreaker.CircuitBreaker
	redis    *redis.Client
}

func NewCircuitBreakerManager(redisClient *redis.Client) *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*gobreaker.CircuitBreaker),
		redis:    redisClient,
	}
}

func (m *CircuitBreakerManager) GetBreaker(webhookID string, errorThreshold int, minRequests int) *gobreaker.CircuitBreaker {
	if b, ok := m.breakers[webhookID]; ok {
		return b
	}
	settings := gobreaker.Settings{
		Name:        webhookID,
		MaxRequests: uint32(minRequests),
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			total := counts.Requests
			fail := counts.TotalFailures
			if total < uint32(minRequests) {
				return false
			}
			failRate := float64(fail) / float64(total) * 100
			return failRate >= float64(errorThreshold)
		},
	}
	breaker := gobreaker.NewCircuitBreaker(settings)
	m.breakers[webhookID] = breaker
	return breaker
}

type CircuitBreaker interface {
	RecordCBResult(ctx context.Context, webhookID string, isSuccess bool) error
	ShouldTrip(ctx context.Context, webhookID string, errorThreshold int, minRequests int, windowSeconds int) (bool, int64, int64, float64, error)
}

type CircuitBreakerManagerAdapter struct {
	mgr *CircuitBreakerManager
}

func NewCircuitBreakerManagerAdapter(mgr *CircuitBreakerManager) *CircuitBreakerManagerAdapter {
	return &CircuitBreakerManagerAdapter{mgr: mgr}
}

func (a *CircuitBreakerManagerAdapter) ShouldTrip(ctx context.Context, webhookID string, errorThreshold int, minRequests int, windowSeconds int) (bool, int64, int64, float64, error) {
	// Check distributed CB first
	cbKey := common.RedisKeyCBPrefix + webhookID
	val, err := a.mgr.redis.Get(ctx, cbKey).Result()
	if err != nil && err != redis.Nil {
		return false, 0, 0, 0, err
	}
	if val == "1" {
		return true, 0, 0, 0, nil
	}

	breaker := a.mgr.GetBreaker(webhookID, errorThreshold, minRequests)
	counts := breaker.Counts()
	total := int64(counts.Requests)
	fail := int64(counts.TotalFailures)
	failRate := 0.0
	if total > 0 {
		failRate = float64(fail) / float64(total) * 100
	}
	shouldTrip := false
	if total >= int64(minRequests) && failRate >= float64(errorThreshold) {
		shouldTrip = true
		// Set distributed CB
		a.mgr.redis.Set(ctx, cbKey, "1", 5*time.Minute)
	}
	return shouldTrip, total - fail, fail, failRate, nil
}
