package pubsub

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"
)

func NewKafkaConsumeApp(kafkaConsumers ...*KafkaConsumer) (*KafkaConsumeApp, error) {
	return &KafkaConsumeApp{
		wg:        &sync.WaitGroup{},
		consumers: kafkaConsumers,
	}, nil
}

type KafkaConsumeApp struct {
	wg *sync.WaitGroup

	consumers []*KafkaConsumer
}

func (k *KafkaConsumeApp) StartConsume(ctx context.Context) error {
	zap.S().Infow("KafkaConsumeApp: StartConsume called")
	errChannel := make(chan error)
	for _, c := range k.consumers {
		go func(c2 *KafkaConsumer) {
			k.wg.Add(1)
			defer k.wg.Done()
			zap.S().Infow("KafkaConsumeApp: calling consumer.consume")
			err := c2.consume(context.Background())
			if err != nil {
				zap.S().Errorw("KafkaConsumeApp: consumer.consume error", "error", err)
				errChannel <- fmt.Errorf("cannot start kafka consumer: %w", err)
				return
			}
			zap.S().Infow("KafkaConsumeApp: consumer.consume finished")
		}(c)
	}

	osKillSignal := make(chan os.Signal, 1)
	var err error
	go func() {
		err = <-errChannel
		if err != nil {
			zap.S().Errorw("start kafka consumer got error", "error", err)
			osKillSignal <- os.Kill
		}
	}()
	go func() {
		<-ctx.Done()
		zap.S().Infow("KafkaConsumeApp: received ctx.Done(), shutting down")
		osKillSignal <- os.Kill
	}()
	zap.S().Infow("KafkaConsumeApp: waiting for shutdown signal")
	signal.Notify(osKillSignal, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-osKillSignal
	zap.S().Infow("KafkaConsumeApp: shutdown signal received, shutting down", "error", err)
	return err
}
