package delivery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"

	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/kafka"
	persistencev1 "gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/persistence"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/types"
)

// Handler processes DeliveryCallback messages from the delivery-status topic
// and persists the outcome via the persistence-service gRPC client.
type Handler struct {
	kafkaService      *kafka.Service
	persistenceClient *persistencev1.DeliveryStatusClient
	operationsTopics  map[string]string
}

// NewHandler creates a delivery callback handler.
func NewHandler(
	kafkaService *kafka.Service,
	persistenceClient *persistencev1.DeliveryStatusClient,
	topics map[string]string,
) *Handler {
	return &Handler{
		kafkaService:      kafkaService,
		persistenceClient: persistenceClient,
		operationsTopics:  topics,
	}
}

// HandleMessage implements kafka.MessageHandler.
func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var callback types.DeliveryCallback
	if err := json.Unmarshal(msg.Value, &callback); err != nil {
		slog.Error("failed to unmarshal delivery callback",
			slog.String("error", err.Error()),
			slog.String("raw", string(msg.Value)),
		)
		return fmt.Errorf("unmarshal delivery callback: %w", err)
	}

	slog.Info("received delivery callback",
		slog.String("notification_id", callback.NotificationID),
		slog.String("channel", callback.Channel),
		slog.String("status", callback.Status),
		slog.String("user_id_at_system", callback.UserIDAtSystem),
	)

	if err := h.persistenceClient.UpdateDeliveryStatus(
		ctx,
		callback.NotificationID,
		callback.UserIDAtSystem,
		callback.Channel,
		callback.Status,
		callback.ErrorMessage,
	); err != nil {
		slog.Error("failed to update delivery status",
			slog.String("notification_id", callback.NotificationID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("update delivery status: %w", err)
	}

	slog.Info("delivery status updated",
		slog.String("notification_id", callback.NotificationID),
		slog.String("channel", callback.Channel),
		slog.String("status", callback.Status),
	)

	return nil
}

// StartConsumer launches the Kafka consumer for the delivery-status topic.
func (h *Handler) StartConsumer(ctx context.Context, workersCount int) {
	topic, ok := h.operationsTopics[string(types.OperationDeliveryStatus)]
	if !ok {
		slog.Warn("no topic configured for delivery_status — consumer not started")
		return
	}

	h.kafkaService.StartConsumer(ctx, kafka.DeliveryStatusConsumer, []string{topic}, h, workersCount)
	slog.Info("delivery status consumer started", slog.String("topic", topic))
}
