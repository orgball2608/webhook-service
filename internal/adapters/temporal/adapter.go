package temporal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/gibson042/canonicaljson-go"
	"github.com/minhvuongrbs/webhook-service/internal/app"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/entities/subscriber"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

type Adapter struct {
	temporalClient client.Client
	taskQueue      string
}

func NewAdapter(temporalClient client.Client, taskQueue string) Adapter {
	return Adapter{temporalClient: temporalClient, taskQueue: taskQueue}
}

func (a Adapter) RegisterWorkflowNotifyEvent(ctx context.Context, e subscriber.Event, priority string) error {
	hash, err := HashEventPayload(e)
	if err != nil {
		return fmt.Errorf("failed to hash event payload: %w", err)
	}
	workflowID := fmt.Sprintf("webhook.notify_event:%s", hash)

	var taskQueue string
	switch priority {
	case common.PriorityHigh:
		taskQueue = common.TaskQueueHigh
	case common.PriorityLow:
		taskQueue = common.TaskQueueLow
	default:
		taskQueue = common.TaskQueueDefault
	}

	wlOpts := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             taskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}

	wlExec, err := a.temporalClient.ExecuteWorkflow(ctx, wlOpts, app.WorkflowNotifyEvent, e)
	if err != nil {
		return fmt.Errorf("failed to execute workflow: %w", err)
	}
	logging.FromContext(ctx).Infow("execute workflow %s using runId %s", wlExec.GetID(), wlExec.GetRunID())
	return nil
}

func HashEventPayload(payload interface{}) (string, error) {
	// Marshal the payload to JSON
	data, err := canonicaljson.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload to JSON: %w", err)
	}

	// Compute the SHA-256 hash
	hash := sha256.Sum256(data)

	// Return the hex-encoded hash
	return hex.EncodeToString(hash[:]), nil
}
