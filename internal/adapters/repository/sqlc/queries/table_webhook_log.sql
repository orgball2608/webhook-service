-- name: ScanDLQForRedrive :many
SELECT id, webhook_id, partner_id, event_payload, redrive_count
FROM webhook_logs
WHERE is_resolved = FALSE
  AND redrive_count < 5
  AND (next_redrive_at IS NULL OR next_redrive_at <= NOW())
LIMIT ?;

-- name: UpdateWebhookLogRedriveStatus :exec
UPDATE webhook_logs
SET redrive_count = redrive_count + 1,
    is_resolved = ?,
    error_message = ?,
    next_redrive_at = ?
WHERE id = ?;

-- name: UpdateWebhookLogStatus :exec
UPDATE webhook_logs
SET status = ?
WHERE id = ?;

-- name: ScanFailedPartners :many
SELECT DISTINCT partner_id
FROM webhook_logs
WHERE is_resolved = FALSE
  AND redrive_count < 5;
