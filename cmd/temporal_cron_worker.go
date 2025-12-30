package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/ports/temporal_workflow"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/metric_server"
	pkgredis "github.com/minhvuongrbs/webhook-service/pkg/redis"
	"github.com/minhvuongrbs/webhook-service/pkg/temporal"
	"github.com/urfave/cli/v2"
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
	fmt.Println("[DEBUG] Using config path:", confPath)
	conf, err := config.LoadConfig(confPath)
	if err != nil {
		return fmt.Errorf("cannot load config")
	}

	// Log toàn bộ biến môi trường liên quan đến Temporal
	fmt.Println("[DEBUG] ENV TEMPORAL_HOST:", os.Getenv("TEMPORAL_HOST"))
	fmt.Println("[DEBUG] ENV TEMPORAL_NAMESPACE:", os.Getenv("TEMPORAL_NAMESPACE"))
	fmt.Println("[DEBUG] ENV TEMPORAL_TASK_QUEUE:", os.Getenv("TEMPORAL_TASK_QUEUE"))
	fmt.Println("[DEBUG] ENV CONFIG_PATH:", os.Getenv("CONFIG_PATH"))
	fmt.Println("[DEBUG] ENVIRONMENT:", os.Getenv("ENV"))

	fmt.Printf("[DEBUG] config.Temporal.Host: %s\n", conf.Temporal.Host)
	fmt.Printf("[DEBUG] config.Temporal.Namespace: %s\n", conf.Temporal.Namespace)
	fmt.Printf("[DEBUG] config.Temporal.TaskQueue: %s\n", conf.Temporal.TaskQueue)

	err = logging.InitLogger(conf.Logger)
	if err != nil {
		return err
	}

	_ = zap.S()
	zap.S().Infow("ENV & PORT MAPPING",
		"DB_HOST", os.Getenv("DB_HOST"),
		"DB_PORT", os.Getenv("DB_PORT"),
		"DB_USER", os.Getenv("DB_USER"),
		"DB_PASS", os.Getenv("DB_PASS"),
		"KAFKA_BROKERS", os.Getenv("KAFKA_BROKERS"),
		"CONFIG_PATH", os.Getenv("CONFIG_PATH"),
		"PORT", os.Getenv("PORT"),
	)

	// Log thêm thông tin cấu hình Temporal để debug
	zap.S().Infow("TEMPORAL CONFIG DEBUG",
		"env", conf.Env,
		"service_name", conf.Logger.ServiceName,
		"temporal_host", conf.Temporal.Host,
		"temporal_namespace", conf.Temporal.Namespace,
		"temporal_task_queue", conf.Temporal.TaskQueue,
		"temporal_tls", conf.Temporal.EnableTLS,
		"temporal_encoder", conf.Temporal.EnableEncoder,
		"temporal_compress", conf.Temporal.EnableCompress,
	)

	fx.New(
		fx.Provide(func() config.Config { return conf }),
		fx.Provide(func(cfg config.Config) database.Config {
			return cfg.Database
		}),
		fx.Provide(func(cfg config.Config) pkgredis.Config {
			return cfg.RedisConnection
		}),
		fx.Provide(pkgredis.NewRedisClient),
		fx.Provide(func(db *sql.DB, redisClient *redis.Client) webhook.Repository {
			return webhook.NewRepository(db, redisClient)
		}),
		fx.Provide(temporal_workflow.NewRedriveActivities),
		fx.Provide(func(conf config.Config) (worker.Worker, error) {
			return temporal.NewTemporalWorker(conf.Temporal, "webhook-cron")
		}),
		fx.Provide(temporal.NewTemporalClient),
		fx.Provide(func(cfg config.Config) temporal.Config {
			return cfg.Temporal
		}),
		fx.Provide(database.NewMysqlDatabaseConn),
		fx.Invoke(func(w worker.Worker, redrive *temporal_workflow.RedriveActivities) {
			temporal_workflow.RegisterCronRedriveWorkflow(w, redrive)
		}),
		fx.Invoke(func(temporalClient client.Client) {
			scheduleClient := temporalClient.ScheduleClient()
			scheduleID := "webhook-cron-redrive-schedule"
			_, err := scheduleClient.Create(context.Background(), client.ScheduleOptions{
				ID: scheduleID,
				Spec: client.ScheduleSpec{
					Intervals: []client.ScheduleIntervalSpec{{
						Every: 30 * time.Minute,
					}},
				},
				Action: &client.ScheduleWorkflowAction{
					Workflow:  "CronRedriveWorkflow",
					ID:        "webhook-cron-redrive-" + time.Now().Format("20060102-150405"),
					TaskQueue: "webhook-cron",
				},
			})
			if err != nil {
				errMsg := strings.ToLower(err.Error())
				if strings.Contains(errMsg, "already exists") {
					zap.S().Infow("webhook cron redrive schedule already exists", "schedule_id", scheduleID)
				} else {
					zap.S().Errorw("failed to create webhook cron redrive schedule", "error", err)
				}
			} else {
				zap.S().Infow("created webhook cron redrive schedule", "schedule_id", scheduleID)
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
