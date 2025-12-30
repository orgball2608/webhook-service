package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minhvuongrbs/webhook-service/config"
	internalcommon "github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/ports/temporal_workflow"
	"github.com/minhvuongrbs/webhook-service/internal/service"
	"github.com/minhvuongrbs/webhook-service/pkg/database"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/metric_server"
	"github.com/minhvuongrbs/webhook-service/pkg/temporal"
	"github.com/urfave/cli/v2"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func startTemporalWorker(cmdCLI *cli.Context) error {
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
	zap.S().Infow("ENV & PORT MAPPING",
		"DB_HOST", os.Getenv("DB_HOST"),
		"DB_PORT", os.Getenv("DB_PORT"),
		"DB_USER", os.Getenv("DB_USER"),
		"DB_PASS", os.Getenv("DB_PASS"),
		"KAFKA_BROKERS", os.Getenv("KAFKA_BROKERS"),
		"CONFIG_PATH", os.Getenv("CONFIG_PATH"),
		"PORT", os.Getenv("PORT"),
	)

	// Log Temporal config for debugging
	zap.L().Info("TEMPORAL CONFIG",
		zap.String("host", conf.Temporal.Host),
		zap.String("namespace", conf.Temporal.Namespace),
		zap.String("task_queue", conf.Temporal.TaskQueue),
	)

	fx.New(
		service.ProvideApplication(conf),
		fx.Provide(temporal_workflow.NewWorkflowNotifyEventToPartner),
		fx.Provide(temporal_workflow.NewWebhookHealthAudit),
		fx.Provide(func(conf config.Config) map[string]worker.Options {
			return map[string]worker.Options{
				internalcommon.QueueCritical: {
					MaxConcurrentActivityExecutionSize: 1000,
					WorkerStopTimeout:                  60 * time.Second,
				},
				internalcommon.QueueDefault: {
					MaxConcurrentActivityExecutionSize: 300,
					WorkerActivitiesPerSecond:          500,
					WorkerStopTimeout:                  60 * time.Second,
				},
				internalcommon.QueueBacklog: {
					MaxConcurrentActivityExecutionSize: 50,
					WorkerActivitiesPerSecond:          20,
					WorkerStopTimeout:                  60 * time.Second,
				},
			}
		}),
		fx.Provide(func(cfg config.Config) database.Config {
			return cfg.Database
		}),
		fx.Invoke(func(queues map[string]worker.Options, workflow *temporal_workflow.NotifyEventToPartner, audit temporal_workflow.WebhookHealthAudit, conf config.Config) {
			metric_server.StartPromAndHealthHTTPServerNoLocking(8088)

			// Create context for graceful shutdown
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Listen for shutdown signals
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				sig := <-sigChan
				zap.L().Info("received signal, initiating graceful shutdown", zap.String("signal", sig.String()))
				cancel()
			}()

			// Start workers
			workers := make([]worker.Worker, 0, len(queues))
			for qName, options := range queues {
				w, err := temporal.NewTemporalWorkerWithOptions(conf.Temporal, qName, options)
				if err != nil {
					panic(fmt.Errorf("init temporal worker for queue %s got error: %w", qName, err))
				}
				workflow.Register(w)
				audit.Register(w)
				workers = append(workers, w)
				go func(w worker.Worker, qName string) {
					// Create interrupt channel for this worker
					interruptCh := make(chan interface{})
					go func() {
						<-ctx.Done()
						close(interruptCh)
					}()
					if err := w.Run(interruptCh); err != nil {
						zap.L().Error("worker failed", zap.String("queue", qName), zap.Error(err))
					}
				}(w, qName)
			}

			// Wait for shutdown signal
			<-ctx.Done()
			zap.L().Info("shutdown signal received, stopping workers gracefully")

			// Stop workers gracefully
			for _, w := range workers {
				w.Stop()
			}
			zap.L().Info("all workers stopped")
		}),
		fx.Invoke(func(temporalClient client.Client) {
			// Always try to create schedule for webhook health audit
			scheduleClient := temporalClient.ScheduleClient()
			scheduleID := "webhook-health-audit-schedule"
			_, err := scheduleClient.Create(context.Background(), client.ScheduleOptions{
				ID: scheduleID,
				Spec: client.ScheduleSpec{
					Intervals: []client.ScheduleIntervalSpec{{
						Every: 30 * time.Minute,
					}},
				},
				Action: &client.ScheduleWorkflowAction{
					Workflow:  "WebhookHealthAuditWorkflow",
					ID:        "webhook-health-audit-" + time.Now().Format("20060102-150405"),
					TaskQueue: internalcommon.QueueCritical,
				},
			})
			if err != nil {
				errMsg := strings.ToLower(err.Error())
				if strings.Contains(errMsg, "already exists") {
					zap.L().Info("webhook health audit schedule already exists", zap.String("schedule_id", scheduleID))
				} else {
					zap.L().Error("failed to create webhook health audit schedule", zap.Error(err))
				}
			} else {
				zap.L().Info("created webhook health audit schedule", zap.String("schedule_id", scheduleID))
			}
		}),
	).Run()

	return nil
}
