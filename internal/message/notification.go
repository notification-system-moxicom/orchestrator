package message

type NotificationMessage struct {
	NotificationId string    `json:"notification_id"`
	SystemId       string    `json:"system_id"`
	UserIds        []string  `json:"user_ids"`
	Channels       []string  `json:"channels"`
	Content        string    `json:"content"`
	Scenario       *Scenario `json:"scenario,omitempty"`
}

type DeliveryTask struct {
	NotificationId string `json:"notification_id"`
	SystemId       string `json:"system_id"`
	UserId         string `json:"user_id"`
	Channel        string `json:"channel"`
	Recipient      string `json:"recipient,omitempty"`
	Content        string `json:"content,omitempty"`
	Subject        string `json:"subject,omitempty"`
}

type Scenario struct {
	Steps []ScenarioStep `json:"steps"`
}

type ScenarioStep struct {
	Channel string `json:"channel"`
	DelayMs int64  `json:"delay_ms"`
}

type DeliveryReceipt struct {
	NotificationId    string `json:"notification_id"`
	SystemId          string `json:"system_id"`
	UserId            string `json:"user_id"`
	Channel           string `json:"channel"`
	Status            string `json:"status"`
	Attempt           int32  `json:"attempt"`
	ProviderMessageId string `json:"provider_message_id,omitempty"`
	Error             string `json:"error,omitempty"`
	SentAt            int64  `json:"sent_at,omitempty"`
}
