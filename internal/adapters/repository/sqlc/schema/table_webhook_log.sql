-- store audit logs for webhook notifications
CREATE TABLE webhook_logs
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    webhook_id      VARCHAR(36) NOT NULL,
    partner_id      VARCHAR(36) NOT NULL,
    event_payload   JSON        NOT NULL COMMENT 'the event data sent to partner',
    response_status INT         NULL COMMENT 'HTTP status code from partner',
    response_body   TEXT        NULL COMMENT 'response body from partner',
    error_message   TEXT        NULL COMMENT 'error message if failed',
    sent_at         TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    redrive_count   INT         NOT NULL DEFAULT 0,
    is_resolved     BOOLEAN     NOT NULL DEFAULT FALSE,
    next_redrive_at TIMESTAMP   NULL,
    status          VARCHAR(20) DEFAULT 'PENDING' COMMENT 'status: PENDING, SUCCESS, FAILED, EXHAUSTED',
    INDEX idx_webhook_id (webhook_id),
    INDEX idx_partner_id (partner_id),
    INDEX idx_sent_at (sent_at),
    INDEX idx_next_redrive (next_redrive_at, is_resolved)
);