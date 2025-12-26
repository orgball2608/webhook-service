package consumer_interceptor

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/google/uuid"
	"github.com/minhvuongrbs/webhook-service/pkg/kafka"
	"github.com/minhvuongrbs/webhook-service/pkg/logging"
	"go.uber.org/zap"
)

func LoggerHandlerWithCtx() kafka.ConsumeHandlerInterceptorWithCtx {
	return func(handler kafka.ConsumeMessageHandlerWithCtx) kafka.ConsumeMessageHandlerWithCtx {
		return func(ctx context.Context, m *kafka.ConsumerMessage) (err error) {
			ll := logging.FromContext(ctx)

			ll.Infow("consume message")
			defer func() {
				if r := recover(); r != nil {
					// include recovered value and stack as string for easier debugging
					ll.Errorw("consume message got panic", "panic_value", r, "stack_trace", string(debug.Stack()))
					// convert panic into an error so the consumer loop can handle it without crashing the whole process
					err = fmt.Errorf("panic in consume handler: %v", r)
				}
			}()

			err = handler(ctx, m)
			if err != nil {
				ll.Errorw("consume message got error", "error", err)
				return err
			}
			ll.Infow("consume message success")
			return nil
		}
	}
}

func AttachLoggerToContext(l *zap.SugaredLogger) kafka.ConsumeHandlerInterceptorWithCtx {
	return func(handler kafka.ConsumeMessageHandlerWithCtx) kafka.ConsumeMessageHandlerWithCtx {
		return func(ctx context.Context, m *kafka.ConsumerMessage) error {
			traceId := m.GetTraceID()
			if traceId == "" {
				//TODO: using otel trace id
				traceId = uuid.NewString()
			}

			ll := l.With("trace_id", traceId, "partition", m.GetPartition(), "offset", m.GetOffset(),
				"created_at", m.GetCreatedAt(), "order_key", m.GetOrderingKey(), "topic", m.GetTopic(),
				"attempt_times", m.GetAttemptTimes())
			ctx = logging.ContextWithLogger(ctx, ll)
			ctx = context.WithValue(ctx, "trace_id", traceId)

			return handler(ctx, m)
		}
	}
}
