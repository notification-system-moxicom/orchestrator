package persistencev1

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DeliveryStatusClient wraps the persistence gRPC client, exposing only
// the UpdateDeliveryStatus method needed by the orchestrator.
type DeliveryStatusClient struct {
	conn           *grpc.ClientConn
	client         PersistenceServiceClient
	requestTimeout time.Duration
}

// NewDeliveryStatusClient dials the persistence-service at the given address.
func NewDeliveryStatusClient(address string, timeout time.Duration) (*DeliveryStatusClient, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("persistence: dial %s: %w", address, err)
	}

	return &DeliveryStatusClient{
		conn:           conn,
		client:         NewPersistenceServiceClient(conn),
		requestTimeout: timeout,
	}, nil
}

// UpdateDeliveryStatus calls the persistence-service RPC to record the delivery
// outcome for a single (notification, user, method) tuple.
func (c *DeliveryStatusClient) UpdateDeliveryStatus(
	ctx context.Context,
	notificationID, userIDAtSystem, method, status, errorMessage string,
) error {
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	_, err := c.client.UpdateDeliveryStatus(ctx, &UpdateDeliveryStatusRequest{
		NotificationId: notificationID,
		UserIdAtSystem: userIDAtSystem,
		Method:         method,
		Status:         status,
		ErrorMessage:   errorMessage,
	})
	if err != nil {
		return fmt.Errorf("persistence.UpdateDeliveryStatus: %w", err)
	}

	return nil
}

// Close releases the gRPC connection.
func (c *DeliveryStatusClient) Close() error {
	return c.conn.Close()
}
