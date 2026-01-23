package kafka

import (
	"context"
	"log/slog"

	"github.com/IBM/sarama"
)

const (
	defaultWorkersCount = 30
)

type ConsumerGroupHandler struct {
	handler      MessageHandler
	workersCount int
}

func NewConsumerHandler(
	messageHandler MessageHandler,
	workers int,
) *ConsumerGroupHandler {
	if workers <= 0 {
		workers = defaultWorkersCount
	}

	return &ConsumerGroupHandler{
		handler:      messageHandler,
		workersCount: workers,
	}
}

func (cgh *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (cgh *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (cgh *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	sem := make(chan struct{}, cgh.workersCount)

	for msg := range claim.Messages() {
		sem <- struct{}{}

		go func(m *sarama.ConsumerMessage) {
			defer func() { <-sem }()

			if err := cgh.handler.HandleMessage(context.Background(), m); err != nil {
				slog.Error("Failed to handle message: %v", err)
			}

			session.MarkMessage(m, "")
		}(msg)
	}

	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	return nil
}
