package types

// SendMethods describes which delivery channels to use.
type SendMethods struct {
	SMS      bool `json:"sms,omitempty"`
	Email    bool `json:"email,omitempty"`
	Telegram bool `json:"telegram,omitempty"`
}

// UserContact holds a user's contact details for delivery.
type UserContact struct {
	IDAtSystem     string `json:"id_at_system"`
	Email          string `json:"email,omitempty"`
	Phone          string `json:"phone,omitempty"`
	TelegramChatID string `json:"telegram_chat_id,omitempty"`
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
