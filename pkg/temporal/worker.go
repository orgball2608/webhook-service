package temporal

import (
	"go.temporal.io/sdk/worker"
)

func NewTemporalWorker(config Config, taskQueue string) (worker.Worker, error) {
	c, err := NewTemporalClient(config)
	if err != nil {
		return nil, err
	}

	w := worker.New(c, taskQueue, worker.Options{
		DisableRegistrationAliasing: true,
	})
	return w, err
}

func NewTemporalWorkerWithOptions(config Config, taskQueue string, opts worker.Options) (worker.Worker, error) {
	c, err := NewTemporalClient(config)
	if err != nil {
		return nil, err
	}

	opts.DisableRegistrationAliasing = true
	w := worker.New(c, taskQueue, opts)
	return w, err
}
