-- store all webhooks
CREATE TABLE webhook
(
    id         VARCHAR(36) PRIMARY KEY,
    status     ENUM ('active', 'inactive', 'pending_verification') NOT NULL DEFAULT 'active',
    partner_id VARCHAR(36)                 NOT NULL,
    metadata   JSON                        NOT NULL DEFAULT (JSON_OBJECT()) COMMENT 'metadata of webhook: name, post_url,...',
    created_at TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- name: UpdateWebhook :execrows
insert into webhook (id, status, partner_id, metadata)
VALUES (?, sqlc.arg(status), sqlc.arg(partner_id), sqlc.arg(metadata))
ON DUPLICATE KEY UPDATE
    status = sqlc.arg(status),
    metadata = sqlc.arg(metadata);

-- name: InsertWebhookLog :exec
INSERT INTO webhook_logs (webhook_id, partner_id, event_payload, response_status, response_body, error_message, status)
VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: GetWebhookById :one
select *
from webhook where id = ?;

-- name: GetWebhooksByIDs :many
SELECT * FROM webhook WHERE id IN (sqlc.slice('webhook_ids'));

-- name: GetActiveWebhooks :many
SELECT * FROM webhook WHERE status = 'active';

-- name: GetActiveWebhooksPaginated :many
SELECT * FROM webhook WHERE status = 'active' ORDER BY id LIMIT ? OFFSET ?;
