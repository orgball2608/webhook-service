package cmd

import (
	"context"
	"fmt"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/ports/temporal_workflow"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/metric_server"
	"github.com/minhvuongrbs/webhook-service/pkg/temporal"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/minhvuongrbs/webhook-service/internal/adapters/repository/webhook"
	"github.com/minhvuongrbs/webhook-service/pkg/database"
	pkgredis "github.com/minhvuongrbs/webhook-service/pkg/redis"
)

func StartTemporalCronWorker(cmdCLI *cli.Context) error {
	confPath := cmdCLI.String("config")
	conf, err := config.LoadConfig(confPath)
	if err != nil {
		return fmt.Errorf("cannot load config")
	}
	err = logging.InitLogger(conf.Logger)
	if err != nil {
		return err
	}
	_ = zap.S()
	metric_server.StartPromAndHealthHTTPServerNoLocking(conf.Monitoring.TemporalWorkerPrometheusPort)

	temporalWorker, err := temporal.NewTemporalWorker(conf.Temporal, "webhook-cron")
	if err != nil {
		return fmt.Errorf("init temporal cron worker got error: %w", err)
	}

	// Khởi tạo repo cho RedriveActivities
	db, err := database.NewMysqlDatabaseConn(conf.Database)
	if err != nil {
		return fmt.Errorf("failed to connect database: %w", err)
	}
	redisClient, err := pkgredis.NewRedisClient(conf.RedisConnection)
	if err != nil {
		return fmt.Errorf("failed to connect redis: %w", err)
	}
	webhookRepo := webhook.NewRepository(db, redisClient)
	redrive := temporal_workflow.NewRedriveActivities(webhookRepo)

	temporal_workflow.RegisterCronRedriveWorkflow(temporalWorker, redrive)

	temporalClient, err := temporal.NewTemporalClient(conf.Temporal)
	if err != nil {
		return fmt.Errorf("failed to create temporal client: %w", err)
	}
	workflowOptions := client.StartWorkflowOptions{
		ID:                    common.CronWorkflowID,
		TaskQueue:             "webhook-cron",
		CronSchedule:          common.CronRedriveSchedule,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	_, err = temporalClient.ExecuteWorkflow(context.Background(), workflowOptions, "CronRedriveWorkflow")
	if err != nil {
		zap.S().Infow("Cron workflow already scheduled or failed to start", "error", err)
	}

	if err = temporalWorker.Run(worker.InterruptCh()); err != nil {
		return err
	}
	return nil
}
