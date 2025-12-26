package partner

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gibson042/canonicaljson-go"
	"github.com/go-resty/resty/v2"
	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/internal/entities/webhook"
)

type Envelope struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type Adapter struct {
	restyClient *resty.Client
	rateLimiter rateLimiter
	logger      Logger
}

type rateLimiter interface {
	ShouldBlock(ctx context.Context, partnerID string, rate int) (bool, error)
}

type Logger interface {
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

func NewAdapter(httpClient http.Client, rateLimiter rateLimiter, logger Logger, timeout time.Duration) Adapter {
	client := resty.NewWithClient(&httpClient)
	client.SetTimeout(timeout)
	return Adapter{
		restyClient: client,
		rateLimiter: rateLimiter,
		logger:      logger,
	}
}

const (
	contentTypeJSON = "application/json"
)

// NotifyWebhookEvent
func (a Adapter) NotifyWebhookEvent(ctx context.Context, w *webhook.Webhook, event subscriber.Event) (*webhook.WebhookResponse, error) {
	a.logger.Info("starting notify webhook event", "url", w.GetPostUrl(), "event_name", event.EventName)

	// Validate inputs
	if w.Metadata.SecretKey == "" {
		a.logger.Error("secret key is empty", "webhook_id", w.Id)
		return nil, fmt.Errorf("secret key is empty")
	}
	if _, err := url.Parse(w.GetPostUrl()); err != nil {
		return nil, fmt.Errorf("invalid post URL: %w", err)
	}

	rate := w.Metadata.GetRateLimitPerMinute()
	isExceed, err := a.rateLimiter.ShouldBlock(ctx, w.PartnerId, rate)
	if err != nil {
		a.logger.Error("rate limiter error", "error", err)
		return nil, fmt.Errorf("rate limiter validation failed: %w", err)
	}
	if isExceed {
		a.logger.Info("rate limit exceeded", "partner_id", w.PartnerId)
		return &webhook.WebhookResponse{
			StatusCode: 429,
			Body:       "",
			Error:      "rate limit exceeded",
		}, nil
	}

	envelope := Envelope{
		Type: string(event.EventName),
		Data: event,
	}
	jsonBody, err := canonicaljson.Marshal(envelope)
	if err != nil {
		a.logger.Error("failed to marshal envelope", "error", err)
		return nil, fmt.Errorf("could not marshal webhook event envelope: %w", err)
	}

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	// Compute HMAC-SHA256 signature: HMAC256(timestamp + "." + payload, secret)
	toSign := timestamp + "." + string(jsonBody)
	signature := computeHMAC256(toSign, w.Metadata.SecretKey)

	// Create EventID (deterministic): hash of event payload
	eventID := generateEventID(jsonBody)
	// Create DeliveryID (unique per delivery): hash of event payload + timestamp
	deliveryID := generateDeliveryID(jsonBody, timestamp)

	a.logger.Info("sending HTTP request", "url", w.GetPostUrl(), "eventID", eventID)
	resp, err := a.restyClient.R().
		SetContext(ctx).
		SetHeader("Content-Type", contentTypeJSON).
		SetHeader("X-Flodesk-Signature", signature).
		SetHeader("X-Flodesk-Timestamp", timestamp).
		SetHeader("X-Flodesk-Event-Id", eventID).
		SetHeader("X-Flodesk-Delivery-Id", deliveryID).
		SetBody(jsonBody).
		EnableTrace().
		Post(w.GetPostUrl())
	if err != nil {
		a.logger.Error("HTTP request failed", "error", err, "url", w.GetPostUrl())
		if resp != nil {
			trace := resp.Request.TraceInfo()
			a.logger.Error("Resty trace", "trace", trace)
		}
		return &webhook.WebhookResponse{
			StatusCode: 0,
			Body:       "",
			Error:      err.Error(),
		}, nil
	}

	// Log Resty trace info for all requests
	trace := resp.Request.TraceInfo()
	a.logger.Info("Resty trace", "trace", trace)

	// Enhanced error handling
	status := resp.StatusCode()
	if status >= 400 && status < 500 {
		if status == 429 {
			a.logger.Info("rate limited by partner", "status", status)
		} else {
			a.logger.Error("client error", "status", status, "body", resp.String())
		}
		return &webhook.WebhookResponse{
				StatusCode: status,
				Body:       resp.String(),
				Error:      "",
			}, &app.WebhookResponseError{
				StatusCode: status,
				Msg:        fmt.Sprintf("partner returned client error: %d", status),
			}
	}
	if status >= 500 {
		a.logger.Error("server error", "status", status, "body", resp.String())
		return &webhook.WebhookResponse{
				StatusCode: status,
				Body:       resp.String(),
				Error:      "",
			}, &app.WebhookResponseError{
				StatusCode: status,
				Msg:        fmt.Sprintf("partner returned server error: %d", status),
			}
	}

	a.logger.Info("webhook sent successfully", "status", status, "eventID", eventID)
	return &webhook.WebhookResponse{
		StatusCode: status,
		Body:       resp.String(),
		Error:      "",
	}, nil
}

func computeHMAC256(data, key string) string {
	h := hmac.New(sha256.New, []byte(key))
	_, _ = h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func generateEventID(payload []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func generateDeliveryID(payload []byte, timestamp string) string {
	h := fnv.New64a()
	_, _ = h.Write(payload)
	_, _ = h.Write([]byte(timestamp))
	return fmt.Sprintf("%x", h.Sum(nil))
}
