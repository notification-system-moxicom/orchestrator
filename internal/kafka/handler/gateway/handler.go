package gateway

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
	"github.com/notification-system-moxicom/orchestrator/internal/types"
	"github.com/notification-system-moxicom/orchestrator/internal/validation"
	pbv1 "github.com/notification-system-moxicom/persistence-service/pkg/proto/gen/persistence/v1"
)

type Handler struct {
	// can add here a service
	gatewayKafka     *kafka.Service
	operationsTopics map[string]string
	adapterTopics    map[string]string
	deliveryDLQ      map[string]string
	validator        *validation.JSONSchemaMessageValidator
	persistence      *persistence.Client
	receiptTopic     string
	scheduler        *scenario.Scheduler
}

func NewHanler(
	kafka *kafka.Service,
	topics map[string]string,
	adapterTopics map[string]string,
	deliveryDLQ map[string]string,
	validator *validation.JSONSchemaMessageValidator,
	persistenceClient *persistence.Client,
	receiptTopic string,
	scheduler *scenario.Scheduler,
) *Handler {
	return &Handler{
		gatewayKafka:     kafka,
		operationsTopics: topics,
		adapterTopics:    adapterTopics,
		deliveryDLQ:      deliveryDLQ,
		validator:        validator,
		persistence:      persistenceClient,
		receiptTopic:     receiptTopic,
		scheduler:        scheduler,
	}
}

// SetScheduler sets the scheduler after construction. This is needed
// to resolve the circular dependency: Handler → Scheduler → Handler.
func (h *Handler) SetScheduler(s *scenario.Scheduler) {
	h.scheduler = s
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

	if h.persistence == nil {
		return fmt.Errorf("persistence client is not configured")
	}

	usersResp, err := h.persistence.GetUsers(ctx, payload.SystemId)
	if err != nil {
		return fmt.Errorf("failed to fetch users from persistence: %w", err)
	}

	userMap := make(map[string]*pbv1.User, len(usersResp.GetUsers()))
	for _, u := range usersResp.GetUsers() {
		userMap[u.GetIdAtSystem()] = u
	}

	uniqueChannels := uniqueStrings(payload.Channels)
	var firstErr error

	for _, externalUserID := range payload.UserIds {
		user, ok := userMap[externalUserID]
		if !ok || user == nil {
			slog.Error("user not found in persistence", "user_id", externalUserID, "system_id", payload.SystemId)
			for _, channel := range uniqueChannels {
				h.sendToDeliveryDLQ(channel, "user_not_found", map[string]any{
					"notification_id": payload.NotificationId,
					"system_id":       payload.SystemId,
					"user_id":         externalUserID,
					"channel":         channel,
				}, fmt.Errorf("user not found"))
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("user not found: %s", externalUserID)
			}
			continue
		}

		internalUserID := user.GetId()
		adapters := user.GetAdapters()
		var email, phone, telegram string
		if adapters != nil {
			email = adapters.GetEmail()
			phone = adapters.GetPhone()
			telegram = adapters.GetTelegramChatId()
		}
		recipientByChannel := map[string]string{
			"email":    email,
			"sms":      phone,
			"telegram": telegram,
		}

		if payload.Scenario != nil && len(payload.Scenario.Steps) > 0 && h.scheduler != nil {
			h.scheduler.StartScenario(payload.NotificationId, payload.SystemId, internalUserID, payload.Scenario.Steps)
			continue
		}

		for _, channel := range uniqueChannels {
			if err := h.sendDeliveryTask(payload, internalUserID, recipientByChannel, channel); err != nil {
				slog.Error("failed to produce delivery task", "error", err, "channel", channel, "user_id", internalUserID)
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

// SendDeliveryTask implements scenario.DeliveryTaskSender.
// It resolves the user's contact for the given channel and produces
// a DeliveryTask to the appropriate adapter Kafka topic.
func (h *Handler) SendDeliveryTask(ctx context.Context, notificationID, systemID, userID, channel string) error {
	topic, ok := h.adapterTopics[channel]
	if !ok || topic == "" {
		err := fmt.Errorf("no adapter topic for channel %s", channel)
		slog.Error("missing adapter topic", "channel", channel)
		h.sendToDeliveryDLQ(channel, "missing_topic", map[string]any{
			"notification_id": notificationID,
			"system_id":       systemID,
			"user_id":         userID,
			"channel":         channel,
		}, err)
		h.emitFailureReceipt(notificationID, systemID, userID, channel, err.Error())
		return err
	}

	// Look up the user's contact info for the requested channel
	usersResp, err := h.persistence.GetUsers(ctx, systemID)
	if err != nil {
		return fmt.Errorf("failed to fetch users for scenario delivery: %w", err)
	}

	var recipient string
	for _, u := range usersResp.GetUsers() {
		if u.GetId() == userID {
			adapters := u.GetAdapters()
			if adapters != nil {
				switch channel {
				case "email":
					recipient = adapters.GetEmail()
				case "sms":
					recipient = adapters.GetPhone()
				case "telegram":
					recipient = adapters.GetTelegramChatId()
				}
			}
			break
		}
	}

	if recipient == "" {
		err := fmt.Errorf("missing recipient for channel %s", channel)
		slog.Error("missing recipient", "channel", channel, "user_id", userID)
		h.sendToDeliveryDLQ(channel, "missing_recipient", map[string]any{
			"notification_id": notificationID,
			"system_id":       systemID,
			"user_id":         userID,
			"channel":         channel,
		}, err)
		h.emitFailureReceipt(notificationID, systemID, userID, channel, err.Error())
		return err
	}

	task := message.DeliveryTask{
		NotificationId: notificationID,
		SystemId:       systemID,
		UserId:         userID,
		Channel:        channel,
		Recipient:      recipient,
		Content:        "",
		Subject:        "Notification",
	}

	if err := h.gatewayKafka.Produce(topic, task); err != nil {
		h.sendToDeliveryDLQ(channel, "produce", task, err)
		return err
	}

	return nil
}

// sendDeliveryTask is the internal version used during direct (non-scenario) message routing.
// It uses the already-fetched recipientByChannel map.
func (h *Handler) sendDeliveryTask(
	payload message.NotificationMessage,
	internalUserID string,
	recipientByChannel map[string]string,
	channel string,
) error {
	topic, ok := h.adapterTopics[channel]
	if !ok || topic == "" {
		err := fmt.Errorf("no adapter topic for channel %s", channel)
		slog.Error("missing adapter topic", "channel", channel)
		h.sendToDeliveryDLQ(channel, "missing_topic", map[string]any{
			"notification_id": payload.NotificationId,
			"system_id":       payload.SystemId,
			"user_id":         internalUserID,
			"channel":         channel,
		}, err)
		h.emitFailureReceipt(payload.NotificationId, payload.SystemId, internalUserID, channel, err.Error())
		return err
	}

	recipient := recipientByChannel[channel]
	if recipient == "" {
		err := fmt.Errorf("missing recipient for channel %s", channel)
		slog.Error("missing recipient", "channel", channel, "user_id", internalUserID)
		h.sendToDeliveryDLQ(channel, "missing_recipient", map[string]any{
			"notification_id": payload.NotificationId,
			"system_id":       payload.SystemId,
			"user_id":         internalUserID,
			"channel":         channel,
		}, err)
		h.emitFailureReceipt(payload.NotificationId, payload.SystemId, internalUserID, channel, err.Error())
		return err
	}

	task := message.DeliveryTask{
		NotificationId: payload.NotificationId,
		SystemId:       payload.SystemId,
		UserId:         internalUserID,
		Channel:        channel,
		Recipient:      recipient,
		Content:        payload.Content,
		Subject:        "Notification",
	}

	if err := h.gatewayKafka.Produce(topic, task); err != nil {
		h.sendToDeliveryDLQ(channel, "produce", task, err)
		return err
	}

	return nil
}

func (h *Handler) emitFailureReceipt(notificationID, systemID, userID, channel, reason string) {
	if h.receiptTopic == "" {
		return
	}

	receipt := message.DeliveryReceipt{
		NotificationId: notificationID,
		SystemId:       systemID,
		UserId:         userID,
		Channel:        channel,
		Status:         "failed",
		Attempt:        0,
		Error:          reason,
		SentAt:         time.Now().UTC().Unix(),
	}

	if err := h.gatewayKafka.Produce(h.receiptTopic, receipt); err != nil {
		slog.Error("failed to emit failure receipt", "error", err, "notification_id", notificationID, "user_id", userID)
	}
}

func (h *Handler) sendToDeliveryDLQ(channel string, stage string, payload any, err error) {
	if h.deliveryDLQ == nil {
		return
	}
	topic := h.deliveryDLQ[channel]
	if topic == "" {
		return
	}

	dlqMsg := map[string]any{
		"stage":     stage,
		"error":     err.Error(),
		"failed_at": time.Now().UTC().Unix(),
		"payload":   payload,
	}

	if err := h.gatewayKafka.Produce(topic, dlqMsg); err != nil {
		slog.Error("failed to produce delivery dlq message", "error", err, "channel", channel)
	}
}
