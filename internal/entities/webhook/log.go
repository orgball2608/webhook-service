package webhook

import "time"

type Log struct {
	ID             int64     `json:"id"`
	WebhookID      string    `json:"webhook_id"`
	PartnerID      string    `json:"partner_id"`
	EventPayload   string    `json:"event_payload"` // JSON string
	ResponseStatus *int      `json:"response_status"`
	ResponseBody   *string   `json:"response_body"`
	ErrorMessage   *string   `json:"error_message"`
	SentAt         time.Time `json:"sent_at"`
	Status         *string   `json:"status"`
}
