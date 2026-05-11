package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/kafka"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/types"
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
		eligibleUsers := filterUsersByMethod(payload.Users, string(method))

		if len(eligibleUsers) == 0 {
			slog.Info("no eligible users for method after filtering",
				slog.String("method", string(method)),
				slog.String("notification_id", payload.NotificationID),
			)
			continue
		}

		deliveryPayload := types.DeliveryPayload{
			NotificationID: payload.NotificationID,
			SystemID:       payload.SystemID,
			Content:        content,
			Method:         method,
			Users:          eligibleUsers,
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
			slog.Int("users_count", len(eligibleUsers)),
		)
	}

	return nil
}

// filterUsersByMethod returns users who have the given method enabled (or have no subscriptions = all enabled)
// and are not in quiet hours.
func filterUsersByMethod(users []types.UserContact, method string) []types.UserContact {
	var result []types.UserContact

	for _, user := range users {
		if !isMethodEnabledForUser(user, method) {
			continue
		}

		if isInQuietHours(user) {
			slog.Info("user in quiet hours, skipping",
				slog.String("user", user.IDAtSystem),
				slog.String("method", method),
			)
			continue
		}

		result = append(result, user)
	}

	return result
}

// isMethodEnabledForUser checks if the user has the given method enabled.
// If no subscriptions are set, all methods are considered enabled (backwards compatible).
func isMethodEnabledForUser(user types.UserContact, method string) bool {
	if len(user.Subscriptions) == 0 {
		return true
	}

	// DeliveryMethod uses "delivery_email" format, subscriptions use "email" format.
	// Normalize by stripping the "delivery_" prefix for comparison.
	channel := strings.TrimPrefix(method, "delivery_")

	for _, sub := range user.Subscriptions {
		if sub.Method == channel {
			return sub.Enabled
		}
	}

	// Method not in subscriptions list = enabled by default
	return true
}

// isInQuietHours checks if the current time falls within the user's quiet hours.
func isInQuietHours(user types.UserContact) bool {
	if user.QuietHours == nil || user.QuietHours.Start == "" || user.QuietHours.End == "" {
		return false
	}

	tz := user.QuietHours.Timezone
	if tz == "" {
		tz = "UTC"
	}

	loc, err := time.LoadLocation(tz)
	if err != nil {
		slog.Warn("invalid timezone for user, ignoring quiet hours",
			slog.String("user", user.IDAtSystem),
			slog.String("timezone", tz),
		)
		return false
	}

	now := time.Now().In(loc)
	currentMinutes := now.Hour()*60 + now.Minute()

	startMinutes, err := parseTimeMinutes(user.QuietHours.Start)
	if err != nil {
		return false
	}

	endMinutes, err := parseTimeMinutes(user.QuietHours.End)
	if err != nil {
		return false
	}

	// Handle overnight quiet hours (e.g., 22:00 - 08:00)
	if startMinutes <= endMinutes {
		return currentMinutes >= startMinutes && currentMinutes < endMinutes
	}

	return currentMinutes >= startMinutes || currentMinutes < endMinutes
}

// parseTimeMinutes parses "HH:MM" into total minutes since midnight.
func parseTimeMinutes(t string) (int, error) {
	parsed, err := time.Parse("15:04", t)
	if err != nil {
		return 0, err
	}

	return parsed.Hour()*60 + parsed.Minute(), nil
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
