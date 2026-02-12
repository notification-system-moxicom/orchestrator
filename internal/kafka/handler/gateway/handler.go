package gateway

import (
	"context"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	"github.com/notification-system-moxicom/orchestrator/internal/types"
)

type Handler struct {
	// can add here a service
	gatewayKafka     *kafka.Service
	operationsTopics map[string]string
}

func NewHanler(kafka *kafka.Service, topics map[string]string) *Handler {
	return &Handler{
		gatewayKafka:     kafka,
		operationsTopics: topics,
	}
}

func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	slog.Info("Handled message", "message", string(msg.Value))
	return nil
}

func (h *Handler) StartConsumer(ctx context.Context, workersCount int) {
	if topic, ok := h.operationsTopics[string(types.OperationSendNotification)]; ok {
		h.gatewayKafka.StartConsumer(ctx, kafka.SendNotificationConsumer, []string{topic}, h, workersCount)
		slog.Info("Consumer started")
	} else {
		slog.Warn("No topic found for sending notification")
	}
}
