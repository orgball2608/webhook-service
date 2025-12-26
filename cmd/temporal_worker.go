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
	metric_server.StartPromAndHealthHTTPServerNoLocking(conf.Monitoring.TemporalWorkerPrometheusPort)

	app, err := service.NewApplication(conf)
	if err != nil {
		return fmt.Errorf("create temporal client application got error: %w", err)
	}
	workerNotifyEventToPartner, err := temporal_workflow.NewWorkflowNotifyEventToPartner(app)
	if err != nil {
		return fmt.Errorf("init worker sync lfvn contract got error: %w", err)
	}

	queues := map[string]int{
		common.TaskQueueDefault: 100,
		common.TaskQueueHigh:    500,
		common.TaskQueueLow:     20,
	}

	for qName, concurrency := range queues {
		w, err := temporal.NewTemporalWorkerWithOptions(conf.Temporal, qName, worker.Options{
			MaxConcurrentActivityExecutionSize: concurrency,
		})
		if err != nil {
			return fmt.Errorf("init temporal worker for queue %s got error: %w", qName, err)
		}
		workerNotifyEventToPartner.Register(w)
		go func(w worker.Worker) {
			if err := w.Run(worker.InterruptCh()); err != nil {
				zap.L().Error("worker failed", zap.String("queue", qName), zap.Error(err))
			}
		}(w)
	}

	// Wait indefinitely
	select {}
}
