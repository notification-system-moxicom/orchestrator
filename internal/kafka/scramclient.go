package kafka

import (
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

var _ sarama.SCRAMClient = (*xdgScramClient)(nil)

type xdgScramClient struct {
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (c *xdgScramClient) Begin(username, password, authzID string) error {
	client, err := c.NewClient(username, password, authzID)
	if err != nil {
		return err
	}

	c.ClientConversation = client.NewConversation()

	return nil
}

func (c *xdgScramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *xdgScramClient) Done() bool {
	return c.ClientConversation.Done()
}
