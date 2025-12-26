package common

const (
	PriorityHigh = "high"
	PriorityLow  = "low"

	TaskQueueHigh    = "webhook-high-priority"
	TaskQueueLow     = "webhook-low-priority"
	TaskQueueDefault = "webhook-default"

	// Redis Keys
	RedisKeyCBPrefix   = "webhook:cb:open:"
	RedisKeyFailStreak = "webhook:fail_streak:"

	// Cron
	CronRedriveSchedule = "*/5 * * * *" // Chạy mỗi 5 phút một lần
	CronWorkflowID      = "webhook.cron.redrive"
)
