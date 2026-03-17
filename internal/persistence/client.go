package persistence

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pbv1 "github.com/notification-system-moxicom/persistence-service/pkg/proto/gen/persistence/v1"
)

type Client struct {
	conn           *grpc.ClientConn
	client         pbv1.PersistenceServiceClient
	requestTimeout time.Duration
}

type ClientConfig struct {
	Address        string
	RequestTimeout time.Duration
}

func NewClient(cfg ClientConfig) (*Client, error) {
	conn, err := grpc.NewClient(
		cfg.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:           conn,
		client:         pbv1.NewPersistenceServiceClient(conn),
		requestTimeout: cfg.RequestTimeout,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) GetUsers(ctx context.Context, systemID string) (*pbv1.Users, error) {
	ctx, cancel := c.contextWithTimeout(ctx)
	defer cancel()

	return c.client.GetUsers(ctx, &pbv1.GetUsersRequest{SystemId: systemID})
}

func (c *Client) ReportDeliveryStatus(ctx context.Context, req *pbv1.ReportDeliveryStatusRequest) (*pbv1.InfoMessage, error) {
	ctx, cancel := c.contextWithTimeout(ctx)
	defer cancel()

	return c.client.ReportDeliveryStatus(ctx, req)
}

func (c *Client) contextWithTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.requestTimeout)
}
