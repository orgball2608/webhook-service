package cmd

import (
	"fmt"

	"github.com/minhvuongrbs/webhook-service/config"
	"github.com/minhvuongrbs/webhook-service/internal/common"
	"github.com/minhvuongrbs/webhook-service/internal/ports/temporal_workflow"
	"github.com/minhvuongrbs/webhook-service/internal/service"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"github.com/minhvuongrbs/webhook-service/pkg/metric_server"
	"github.com/minhvuongrbs/webhook-service/pkg/temporal"
	"github.com/urfave/cli/v2"
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

	fx.New(
		service.ProvideApplication(conf),
		fx.Provide(temporal_workflow.NewWorkflowNotifyEventToPartner),
		fx.Provide(func(conf config.Config) map[string]int {
			return map[string]int{
				common.TaskQueueDefault: 100,
				common.TaskQueueHigh:    500,
				common.TaskQueueLow:     20,
			}
		}),
		fx.Invoke(func(queues map[string]int, workflow *temporal_workflow.NotifyEventToPartner, conf config.Config) {
			metric_server.StartPromAndHealthHTTPServerNoLocking(conf.Monitoring.TemporalWorkerPrometheusPort)
			for qName, concurrency := range queues {
				w, err := temporal.NewTemporalWorkerWithOptions(conf.Temporal, qName, worker.Options{
					MaxConcurrentActivityExecutionSize: concurrency,
				})
				if err != nil {
					panic(fmt.Errorf("init temporal worker for queue %s got error: %w", qName, err))
				}
				workflow.Register(w)
				go func(w worker.Worker) {
					if err := w.Run(worker.InterruptCh()); err != nil {
						zap.L().Error("worker failed", zap.String("queue", qName), zap.Error(err))
					}
				}(w)
			}
		}),
	).Run()

	// Wait indefinitely
	select {}
}
