package common

const (
	PriorityHigh = "high"
	PriorityLow  = "low"

	QueueCritical = "webhook-critical"
	QueueDefault  = "webhook-default"
	QueueBacklog  = "webhook-backlog"

	// Redis Keys
	RedisKeyCBPrefix    = "webhook:cb:open:"
	RedisKeyFailRate    = "webhook:fail_rate:"
	RedisKeySuccessRate = "webhook:success_rate:"
	RedisKey4xxRate     = "webhook:4xx_rate:"

	// Cron
	CronRedriveSchedule = "*/5 * * * *" // Chạy mỗi 5 phút một lần
	CronWorkflowID      = "webhook.cron.redrive"
)
