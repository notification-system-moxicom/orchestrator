package types

// SendMethods describes which delivery channels to use.
type SendMethods struct {
	SMS      bool `json:"sms,omitempty"`
	Email    bool `json:"email,omitempty"`
	Telegram bool `json:"telegram,omitempty"`
}

// SubscriptionPayload holds a user's channel subscription preference.
type SubscriptionPayload struct {
	Method   string `json:"method"`
	Enabled  bool   `json:"enabled"`
	Priority int32  `json:"priority"`
}

// QuietHoursPayload holds a user's do-not-disturb settings.
type QuietHoursPayload struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	Timezone string `json:"timezone"`
}

// UserContact holds a user's contact details for delivery.
type UserContact struct {
	IDAtSystem     string                `json:"id_at_system"`
	Email          string                `json:"email,omitempty"`
	Phone          string                `json:"phone,omitempty"`
	TelegramChatID string                `json:"telegram_chat_id,omitempty"`
	Subscriptions  []SubscriptionPayload `json:"subscriptions,omitempty"`
	QuietHours     *QuietHoursPayload    `json:"quiet_hours,omitempty"`
}

// ScenarioPayload carries scenario/template metadata.
type ScenarioPayload struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Template string `json:"template,omitempty"`
	Settings string `json:"settings,omitempty"`
}

// SendNotificationPayload is the message produced by api-gateway
// into the send_notification_orchestration topic.
type SendNotificationPayload struct {
	NotificationID string           `json:"notification_id"`
	SystemID       string           `json:"system_id"`
	Content        string           `json:"content"`
	Methods        *SendMethods     `json:"methods,omitempty"`
	Users          []UserContact    `json:"users"`
	Scenario       *ScenarioPayload `json:"scenario,omitempty"`
	TemplateParams map[string]any   `json:"template_params,omitempty"`
}

// DeliveryPayload is the message produced by orchestrator
// into delivery-specific topics (email, telegram, sms).
type DeliveryPayload struct {
	NotificationID string         `json:"notification_id"`
	SystemID       string         `json:"system_id"`
	Content        string         `json:"content"`
	Method         DeliveryMethod `json:"method"`
	Users          []UserContact  `json:"users"`
}

// DeliveryCallback is the unified callback message sent by all delivery adapters
// to the delivery-status topic. The Channel field distinguishes which adapter sent it.
type DeliveryCallback struct {
	NotificationID string `json:"notification_id"`
	SystemID       string `json:"system_id"`
	UserIDAtSystem string `json:"user_id_at_system"`
	Channel        string `json:"channel"` // "telegram", "email", "sms", "push"
	Status         string `json:"status"`  // "sent", "failed"
	ErrorMessage   string `json:"error_message,omitempty"`
	DeliveredAt    int64  `json:"delivered_at"`
}
