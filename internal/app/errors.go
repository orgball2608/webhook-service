package app

import "errors"

var (
	ErrPartnerTerminal  = errors.New("partner terminal error")  // 401, 404, 400
	ErrPartnerTransient = errors.New("partner transient error") // 5xx, Timeout, 429
)

type WebhookResponseError struct {
	StatusCode int
	Msg        string
}

func (e *WebhookResponseError) Error() string { return e.Msg }
