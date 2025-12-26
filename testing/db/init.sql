-- create database
CREATE DATABASE webhook;
-- create table webhook
CREATE TABLE webhook
(
    id         VARCHAR(36) PRIMARY KEY,
    status     ENUM ('active', 'inactive') NOT NULL DEFAULT 'active',
    partner_id VARCHAR(36)                 NOT NULL,
    metadata   JSON                        NOT NULL DEFAULT (JSON_OBJECT()) COMMENT 'metadata of webhook: name, post_url,...',
    created_at TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Add 20 mocks records
INSERT INTO webhook.webhook (id, status, partner_id, metadata, created_at, updated_at) VALUES
('c84gk48qvh2l9t5dzp3f', 'active', 'cpaa1fg4uq1ne39r9p21', '{"name": "webhook name1", "events": ["subscriber.created", "subscriber.subscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:04:36', '2024-05-27 15:04:36'),
('b17t5g83ch2m6a4hjb9e', 'active', 'cpaa1fg4uq1ne39r9p22', '{"name": "webhook name2", "events": ["subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:05:36', '2024-05-27 15:05:36'),
('f72nj40exk7s1p8wrm0t', 'inactive', 'cpaa1fg4uq1ne39r9p23', '{"name": "webhook name3", "events": ["subscriber.created"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:06:36', '2024-05-27 15:06:36'),
('d35sk27lvh4c2j5azr8u', 'active', 'cpaa1fg4uq1ne39r9p24', '{"name": "webhook name4", "events": ["subscriber.subscribed", "subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:07:36', '2024-05-27 15:07:36'),
('e90fj34oxk6r2p7vhc1y', 'active', 'cpaa1fg4uq1ne39r9p25', '{"name": "webhook name5", "events": ["subscriber.created", "subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:08:36', '2024-05-27 15:08:36'),
('a24gk57qvh1n8t5dzp6f', 'inactive', 'cpaa1fg4uq1ne39r9p26', '{"name": "webhook name6", "events": ["subscriber.created"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:09:36', '2024-05-27 15:09:36'),
('g83t5j72ch4m6a5hjb9e', 'active', 'cpaa1fg4uq1ne39r9p27', '{"name": "webhook name7", "events": ["subscriber.subscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:10:36', '2024-05-27 15:10:36'),
('h19nj50dxk3s1p8wrm2t', 'inactive', 'cpaa1fg4uq1ne39r9p28', '{"name": "webhook name8", "events": ["subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:11:36', '2024-05-27 15:11:36'),
('i64sk36lvh2c5j6azr4u', 'active', 'cpaa1fg4uq1ne39r9p29', '{"name": "webhook name9", "events": ["subscriber.created", "subscriber.subscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:12:36', '2024-05-27 15:12:36'),
('j20fj44oxk9r2p7vhc3y', 'active', 'cpaa1fg4uq1ne39r9p30', '{"name": "webhook name10", "events": ["subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:13:36', '2024-05-27 15:13:36'),
('k54gk38qvh6l7t5dzp2f', 'active', 'cpaa1fg4uq1ne39r9p31', '{"name": "webhook name11", "events": ["subscriber.created", "subscriber.subscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:14:36', '2024-05-27 15:14:36'),
('l32t5g92ch8m4a3hjb7e', 'active', 'cpaa1fg4uq1ne39r9p32', '{"name": "webhook name12", "events": ["subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:15:36', '2024-05-27 15:15:36'),
('m62nj45exk2s1p9wrm5t', 'inactive', 'cpaa1fg4uq1ne39r9p33', '{"name": "webhook name13", "events": ["subscriber.created"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:16:36', '2024-05-27 15:16:36'),
('n47sk25lvh5c2j4azr1u', 'active', 'cpaa1fg4uq1ne39r9p34', '{"name": "webhook name14", "events": ["subscriber.subscribed", "subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:17:36', '2024-05-27 15:17:36'),
('o21fj49oxk7r3p6vhc8y', 'active', 'cpaa1fg4uq1ne39r9p35', '{"name": "webhook name15", "events": ["subscriber.created", "subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:18:36', '2024-05-27 15:18:36'),
('p73gk52qvh9l6t4dzp1f', 'inactive', 'cpaa1fg4uq1ne39r9p36', '{"name": "webhook name16", "events": ["subscriber.created"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:19:36', '2024-05-27 15:19:36'),
('q18t5j81ch3m7a2hjb4e', 'active', 'cpaa1fg4uq1ne39r9p37', '{"name": "webhook name17", "events": ["subscriber.subscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:20:36', '2024-05-27 15:20:36'),
('r91nj54dxk5s1p9wrm3t', 'inactive', 'cpaa1fg4uq1ne39r9p38', '{"name": "webhook name18", "events": ["subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:21:36', '2024-05-27 15:21:36'),
('s53sk29lvh8c4j3azr6u', 'active', 'cpaa1fg4uq1ne39r9p39', '{"name": "webhook name19", "events": ["subscriber.created", "subscriber.subscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:22:36', '2024-05-27 15:22:36'),
('t29fj42oxk6r4p8vhc5y', 'active', 'cpaa1fg4uq1ne39r9p40', '{"name": "webhook name20", "events": ["subscriber.unsubscribed"], "post_url": "https://webhook-test.com/0bc38cfb11771e4d13548825c93249ec", "rate_limit_per_minute": 100, "secret_key": "webhook_secret", "error_threshold_percentage": 90, "min_requests_to_trip": 20, "evaluation_window_seconds": 300}', '2024-05-27 15:23:36', '2024-05-27 15:23:36');

-- create table webhook_logs
CREATE TABLE webhook_logs
(
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    webhook_id        VARCHAR(36) NOT NULL,
    partner_id        VARCHAR(36) NOT NULL,
    event_payload     TEXT        NOT NULL,
    response_status   INT,
    response_body     TEXT,
    error_message     TEXT,
    status            VARCHAR(50),
    is_resolved       BOOLEAN     DEFAULT FALSE,
    redrive_count     INT         DEFAULT 0,
    next_redrive_at   TIMESTAMP   NULL,
    created_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_is_resolved (is_resolved),
    INDEX idx_next_redrive (next_redrive_at, is_resolved)
);
