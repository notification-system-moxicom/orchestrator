package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

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
	var payload types.SendNotificationPayload
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		slog.Error("failed to unmarshal send notification payload",
			slog.String("error", err.Error()),
			slog.String("raw", string(msg.Value)),
		)
		return fmt.Errorf("unmarshal send notification payload: %w", err)
	}

	slog.Info("received send notification",
		slog.String("notification_id", payload.NotificationID),
		slog.String("system_id", payload.SystemID),
		slog.Int("users_count", len(payload.Users)),
	)

	if err := h.processNotification(ctx, &payload); err != nil {
		slog.Error("failed to process notification",
			slog.String("notification_id", payload.NotificationID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("process notification %s: %w", payload.NotificationID, err)
	}

	return nil
}

func (h *Handler) processNotification(ctx context.Context, payload *types.SendNotificationPayload) error {
	content := payload.Content

	// Apply template if scenario is provided
	if payload.Scenario != nil && payload.Scenario.Template != "" {
		rendered, err := applyTemplate(payload.Scenario.Template, payload.TemplateParams)
		if err != nil {
			slog.Warn("template rendering failed, using raw content",
				slog.String("notification_id", payload.NotificationID),
				slog.String("error", err.Error()),
			)
		} else {
			content = rendered
		}
	}

	methods := resolveDeliveryMethods(payload)

	for _, method := range methods {
		deliveryPayload := types.DeliveryPayload{
			NotificationID: payload.NotificationID,
			SystemID:       payload.SystemID,
			Content:        content,
			Method:         method,
			Users:          payload.Users,
		}

		topic, ok := h.operationsTopics[string(method)]
		if !ok {
			slog.Warn("no topic configured for delivery method",
				slog.String("method", string(method)),
				slog.String("notification_id", payload.NotificationID),
			)
			continue
		}

		if err := h.gatewayKafka.Produce(topic, deliveryPayload); err != nil {
			slog.Error("failed to produce delivery message",
				slog.String("topic", topic),
				slog.String("method", string(method)),
				slog.String("notification_id", payload.NotificationID),
				slog.String("error", err.Error()),
			)
			return fmt.Errorf("produce to %s: %w", topic, err)
		}

		slog.Info("delivery message produced",
			slog.String("topic", topic),
			slog.String("method", string(method)),
			slog.String("notification_id", payload.NotificationID),
		)
	}

	return nil
}

func resolveDeliveryMethods(payload *types.SendNotificationPayload) []types.DeliveryMethod {
	if payload.Methods == nil {
		return []types.DeliveryMethod{types.DeliveryEmail}
	}

	var methods []types.DeliveryMethod
	if payload.Methods.Email {
		methods = append(methods, types.DeliveryEmail)
	}
	if payload.Methods.Telegram {
		methods = append(methods, types.DeliveryTelegram)
	}
	if payload.Methods.SMS {
		methods = append(methods, types.DeliverySMS)
	}

	if len(methods) == 0 {
		methods = append(methods, types.DeliveryEmail)
	}

	return methods
}

func applyTemplate(tmpl string, params map[string]any) (string, error) {
	if len(params) == 0 {
		return tmpl, nil
	}

	result := tmpl
	for key, val := range params {
		placeholder := "{{" + key + "}}"
		result = strings.ReplaceAll(result, placeholder, fmt.Sprintf("%v", val))
	}

	return result, nil
}

func (h *Handler) StartConsumer(ctx context.Context, workersCount int) {
	if topic, ok := h.operationsTopics[string(types.OperationSendNotification)]; ok {
		h.gatewayKafka.StartConsumer(ctx, kafka.SendNotificationConsumer, []string{topic}, h, workersCount)
		slog.Info("Consumer started")
	} else {
		slog.Warn("No topic found for sending notification")
	}
}
