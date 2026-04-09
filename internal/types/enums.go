package types

type Operation string

const (
	OperationSendNotification Operation = "send_notification_orchestration"
)

type DeliveryMethod string

const (
	DeliveryEmail    DeliveryMethod = "delivery_email"
	DeliveryTelegram DeliveryMethod = "delivery_telegram"
	DeliverySMS      DeliveryMethod = "delivery_sms"
)
