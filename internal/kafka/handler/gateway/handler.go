package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	"github.com/notification-system-moxicom/orchestrator/internal/message"
	"github.com/notification-system-moxicom/orchestrator/internal/types"
	"github.com/notification-system-moxicom/orchestrator/internal/validation"
)

type Handler struct {
	// can add here a service
	gatewayKafka     *kafka.Service
	operationsTopics map[string]string
	adapterTopics    map[string]string
	validator        *validation.JSONSchemaMessageValidator
}

func NewHanler(kafka *kafka.Service, topics map[string]string, adapterTopics map[string]string, validator *validation.JSONSchemaMessageValidator) *Handler {
	return &Handler{
		gatewayKafka:     kafka,
		operationsTopics: topics,
		adapterTopics:    adapterTopics,
		validator:        validator,
	}
}

func (h *Handler) HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var payload message.NotificationMessage
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("failed to unmarshal gateway message: %w", err)
	}

	if h.validator != nil {
		if err := h.validator.Validate(&payload); err != nil {
			return fmt.Errorf("invalid gateway message: %w", err)
		}
	}

	if len(payload.UserIds) == 0 {
		return fmt.Errorf("gateway message has no user_ids")
	}
	if len(payload.Channels) == 0 {
		return fmt.Errorf("gateway message has no channels")
	}

	uniqueChannels := uniqueStrings(payload.Channels)
	var firstErr error

	for _, userID := range payload.UserIds {
		for _, channel := range uniqueChannels {
			topic, ok := h.adapterTopics[channel]
			if !ok || topic == "" {
				err := fmt.Errorf("no adapter topic for channel %s", channel)
				slog.Error("missing adapter topic", "channel", channel)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			task := message.DeliveryTask{
				NotificationId: payload.NotificationId,
				SystemId:       payload.SystemId,
				UserId:         userID,
				Channel:        channel,
			}

			if err := h.gatewayKafka.Produce(topic, task); err != nil {
				slog.Error("failed to produce delivery task", "error", err, "channel", channel, "user_id", userID)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	return firstErr
}

func (h *Handler) StartConsumer(ctx context.Context, workersCount int) {
	if topic, ok := h.operationsTopics[string(types.OperationSendNotification)]; ok {
		h.gatewayKafka.StartConsumer(ctx, kafka.SendNotificationConsumer, []string{topic}, h, workersCount)
		slog.Info("Consumer started")
	} else {
		slog.Warn("No topic found for sending notification")
	}
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
