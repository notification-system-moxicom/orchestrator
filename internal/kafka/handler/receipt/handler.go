package receipt

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"

	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	"github.com/notification-system-moxicom/orchestrator/internal/message"
	"github.com/notification-system-moxicom/orchestrator/internal/persistence"
	"github.com/notification-system-moxicom/orchestrator/internal/scenario"
	"github.com/notification-system-moxicom/orchestrator/internal/validation"
	pbv1 "github.com/notification-system-moxicom/persistence-service/pkg/proto/gen/persistence/v1"
)

type Handler struct {
	kafka       *kafka.Service
	receiptTopic string
	dlqTopic    string
	validator   *validation.JSONSchemaMessageValidator
	persistence *persistence.Client
	scheduler   *scenario.Scheduler
}

func NewHandler(
	kafkaService *kafka.Service,
	receiptTopic string,
	dlqTopic string,
	validator *validation.JSONSchemaMessageValidator,
	persistenceClient *persistence.Client,
	scheduler *scenario.Scheduler,
) *Handler {
	return &Handler{
		kafka:       kafkaService,
		receiptTopic: receiptTopic,
		dlqTopic:    dlqTopic,
		validator:   validator,
		persistence: persistenceClient,
		scheduler:   scheduler,
	}
}

func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var payload message.DeliveryReceipt
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		h.sendToDLQ(msg.Value, "unmarshal", err)
		return nil
	}

	if h.validator != nil {
		if err := h.validator.Validate(&payload); err != nil {
			h.sendToDLQ(msg.Value, "validation", err)
			return nil
		}
	}

	if h.persistence == nil {
		return fmt.Errorf("persistence client is not configured")
	}

	req := &pbv1.ReportDeliveryStatusRequest{
		NotificationId:    payload.NotificationId,
		UserId:            payload.UserId,
		Channel:           payload.Channel,
		Status:            payload.Status,
		Attempt:           payload.Attempt,
		ProviderMessageId: payload.ProviderMessageId,
		Error:             payload.Error,
		SentAt:            payload.SentAt,
	}

	if _, err := h.persistence.ReportDeliveryStatus(ctx, req); err != nil {
		h.sendToDLQ(msg.Value, "persistence", err)
		return nil
	}

	if h.scheduler != nil {
		switch payload.Status {
		case "sent":
			skipped := h.scheduler.MarkSuccess(payload.NotificationId, payload.UserId, payload.Channel)
			if len(skipped) > 0 {
				h.emitSkippedReceipts(payload, skipped)
			}
		case "failed":
			h.scheduler.MarkFailure(payload.NotificationId, payload.UserId, payload.Channel)
		}
	}

	return nil
}

func (h *Handler) sendToDLQ(payload []byte, stage string, err error) {
	if h.dlqTopic == "" {
		slog.Error("dlq topic is not configured", "stage", stage, "error", err)
		return
	}

	dlqMsg := map[string]any{
		"stage":     stage,
		"error":     err.Error(),
		"failed_at": time.Now().UTC().Unix(),
		"payload":   json.RawMessage(payload),
	}

	if err := h.kafka.Produce(h.dlqTopic, dlqMsg); err != nil {
		slog.Error("failed to produce dlq message", "stage", stage, "error", err)
	}
}

func (h *Handler) emitSkippedReceipts(original message.DeliveryReceipt, steps []message.ScenarioStep) {
	if h.receiptTopic == "" {
		slog.Error("receipt topic is not configured for skipped receipts", "notification_id", original.NotificationId, "user_id", original.UserId)
		return
	}

	for _, step := range steps {
		receipt := message.DeliveryReceipt{
			NotificationId: original.NotificationId,
			SystemId:       original.SystemId,
			UserId:         original.UserId,
			Channel:        step.Channel,
			Status:         "skipped",
			Attempt:        0,
			Error:          "skipped due to scenario success",
			SentAt:         time.Now().UTC().Unix(),
		}

		if err := h.kafka.Produce(h.receiptTopic, receipt); err != nil {
			slog.Error("failed to emit skipped receipt", "error", err, "notification_id", original.NotificationId, "user_id", original.UserId, "channel", step.Channel)
		}
	}
}
