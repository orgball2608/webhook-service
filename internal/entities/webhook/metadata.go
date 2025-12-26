package webhook

import "github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"

type Metadata struct {
	Name               string                 `json:"name"`
	PostUrl            string                 `json:"post_url"`
	RateLimitPerMinute int                    `json:"rate_limit_per_minute"`
	SecretKey          string                 `json:"secret_key"`
	Events             []subscriber.EventName `json:"events"`
	Priority           string                 `json:"priority"`
	// Circuit Breaker config
	ErrorThresholdPercentage int `json:"error_threshold_percentage"`
	MinRequestsToTrip        int `json:"min_requests_to_trip"`
	EvaluationWindowSeconds  int `json:"evaluation_window_seconds"`
	RedriveCount             int `json:"redrive_count"`
}

func (m Metadata) GetPostUrl() string {
	return m.PostUrl
}

func (m Metadata) GetRateLimitPerMinute() int {
	if m.RateLimitPerMinute == 0 {
		return 1000
	}

	return m.RateLimitPerMinute
}
