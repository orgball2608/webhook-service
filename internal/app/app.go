package app

import "github.com/redis/go-redis/v9"

type App struct {
	NotifyEventHandler         NotifyEventHandler
	RegisterNotifyEventHandler RegisterNotifyEventHandler
	RedisClient                *redis.Client
	RateLimiter                RateLimiter
	PartnerAdapter             PartnerAdapter
	CircuitBreakerManager      CircuitBreaker
}
