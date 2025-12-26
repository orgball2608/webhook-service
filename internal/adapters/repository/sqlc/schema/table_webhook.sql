-- store all webhooks
CREATE TABLE IF NOT EXISTS webhook (
    id VARCHAR(36) PRIMARY KEY,
    partner_id VARCHAR(36) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'PAUSED')),
    metadata JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Metadata mẫu:
-- {
--   "rate_limit_per_minute": 20,
--   "error_threshold_percentage": 90,
--   "min_requests_to_trip": 20,
--   "evaluation_window_seconds": 300
-- }
