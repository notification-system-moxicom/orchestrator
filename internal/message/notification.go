package message

type NotificationMessage struct {
	NotificationId string   `json:"notification_id"`
	SystemId       string   `json:"system_id"`
	UserIds        []string `json:"user_ids"`
	Channels       []string `json:"channels"`
}

type DeliveryTask struct {
	NotificationId string `json:"notification_id"`
	SystemId       string `json:"system_id"`
	UserId         string `json:"user_id"`
	Channel        string `json:"channel"`
}
