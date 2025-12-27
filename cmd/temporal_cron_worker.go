package cmd

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/ports/temporal_workflow"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/metric_server"
	pkgredis "github.com/minhvuongrbs/webhook-service/pkg/redis"
	"github.com/minhvuongrbs/webhook-service/pkg/temporal"
	"github.com/urfave/cli/v2"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/minhvuongrbs/webhook-service/internal/adapters/repository/webhook"
	"github.com/minhvuongrbs/webhook-service/pkg/database"
	"github.com/redis/go-redis/v9"
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

	fx.New(
		fx.Provide(func() config.Config { return conf }),
		fx.Provide(database.NewMysqlDatabaseConn),
		fx.Provide(pkgredis.NewRedisClient),
		fx.Provide(func(db *sql.DB, redisClient *redis.Client) webhook.Repository {
			return webhook.NewRepository(db, redisClient)
		}),
		fx.Provide(temporal_workflow.NewRedriveActivities),
		fx.Provide(func(conf config.Config) (worker.Worker, error) {
			return temporal.NewTemporalWorker(conf.Temporal, "webhook-cron")
		}),
		fx.Provide(temporal.NewTemporalClient),
		fx.Invoke(func(w worker.Worker, redrive *temporal_workflow.RedriveActivities) {
			temporal_workflow.RegisterCronRedriveWorkflow(w, redrive)
		}),
		fx.Invoke(func(temporalClient client.Client, conf config.Config) {
			workflowOptions := client.StartWorkflowOptions{
				ID:                    common.CronWorkflowID,
				TaskQueue:             "webhook-cron",
				CronSchedule:          common.CronRedriveSchedule,
				WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			}
			_, err := temporalClient.ExecuteWorkflow(context.Background(), workflowOptions, "CronRedriveWorkflow")
			if err != nil {
				zap.S().Infow("Cron workflow already scheduled or failed to start", "error", err)
			}
		}),
		fx.Invoke(func() {
			metric_server.StartPromAndHealthHTTPServerNoLocking(conf.Monitoring.TemporalWorkerPrometheusPort)
		}),
		fx.Invoke(func(w worker.Worker) {
			if err := w.Run(worker.InterruptCh()); err != nil {
				panic(err)
			}
		}),
	).Run()

	return nil
}
