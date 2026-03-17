package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/notification-system-moxicom/orchestrator/internal/config"
	"github.com/notification-system-moxicom/orchestrator/internal/http/handlers"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka/handler/gateway"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka/handler/receipt"
	"github.com/notification-system-moxicom/orchestrator/internal/persistence"
	redisclient "github.com/notification-system-moxicom/orchestrator/internal/redis"
	"github.com/notification-system-moxicom/orchestrator/internal/scenario"
	"github.com/notification-system-moxicom/orchestrator/internal/server"
	"github.com/notification-system-moxicom/orchestrator/internal/validation"
	"github.com/notification-system-moxicom/orchestrator/pkg/logger"
)

func main() {
	var configPath string

	ctx := context.Background()
	logger.SetDefaults(nil)
	flag.StringVar(&configPath, "c", "config.yaml", "Set path to config file.")
	flag.Parse()

	cfg, err := config.ReadConfig(configPath)
	if err != nil {
		slog.Error("can't configure from config file:", slog.String("error", err.Error()))
		return
	}

	schemaFiles := map[string]string{
		"NotificationMessage": "schemas/send_notification.json",
		"DeliveryReceipt":     "schemas/delivery_receipt.json",
	}
	validator, err := validation.NewJSONSchemaMessageValidator(schemaFiles)
	if err != nil {
		slog.Error("failed to create JSON schema validator:", slog.String("error", err.Error()))
		return
	}

	persistenceClient, err := persistence.NewClient(persistence.ClientConfig{
		Address:        cfg.Integrations.RPC.Persistence.Address,
		RequestTimeout: cfg.Integrations.RPC.Persistence.RequestTimeout,
	})
	if err != nil {
		slog.Error("failed to create persistence client:", slog.String("error", err.Error()))
		return
	}

	defer func() {
		slog.Info("Closing persistence client...")
		if err := persistenceClient.Close(); err != nil {
			slog.Error("Error closing persistence client: ", err)
		}
	}()

	gatewayKafka, err := kafka.NewService(&cfg.Connections.Kafka.Gateway, validator)
	if err != nil {
		slog.Error("failed to create Kafka service:", slog.String("error", err.Error()))
		return
	}

	// Redis connection for scenario scheduler
	rdb, err := redisclient.NewClient(cfg.Connections.Redis)
	if err != nil {
		slog.Error("failed to create Redis client:", slog.String("error", err.Error()))
		return
	}
	defer func() {
		slog.Info("Closing Redis client...")
		if err := rdb.Close(); err != nil {
			slog.Error("Error closing Redis client:", slog.String("error", err.Error()))
		}
	}()

	// The gateway handler acts as the DeliveryTaskSender for the scheduler.
	// We create the handler first, then pass it to the scheduler.
	apiGatewayHandler := gateway.NewHanler(
		gatewayKafka,
		cfg.Settings.OperationsTopics,
		cfg.Settings.AdapterTopics,
		cfg.Settings.DLQTopics.Deliveries,
		validator,
		persistenceClient,
		cfg.Settings.ReceiptTopic,
		nil, // scheduler will be set below
	)

	scheduler := scenario.NewScheduler(rdb, cfg.Connections.Redis.ScenarioPollInterval, apiGatewayHandler)
	scheduler.StartPoller(ctx)
	defer scheduler.Stop()

	// Wire the scheduler back into the handler
	apiGatewayHandler.SetScheduler(scheduler)

	apiGatewayHandler.StartConsumer(ctx, cfg.Connections.Kafka.Gateway.ConsumerWorkersCount)

	if cfg.Settings.ReceiptTopic != "" {
		receiptHandler := receipt.NewHandler(
			gatewayKafka,
			cfg.Settings.ReceiptTopic,
			cfg.Settings.DLQTopics.Callbacks,
			validator,
			persistenceClient,
			scheduler,
		)
		gatewayKafka.StartConsumer(
			ctx,
			kafka.DeliveryReceiptConsumer,
			[]string{cfg.Settings.ReceiptTopic},
			receiptHandler,
			cfg.Connections.Kafka.Gateway.ConsumerWorkersCount,
		)
	} else {
		slog.Warn("receipt topic is not configured, callbacks consumer disabled")
	}

	defer func() {
		slog.Info("Closing gatewayKafka Kafka producer...")

		if err := gatewayKafka.CloseProducer(); err != nil {
			slog.Error("Error closing gatewayKafka Kafka producer: ", err)
		}

		slog.Info("gatewayKafka producer closed successfully")
	}()

	defer func() {
		slog.Info("Closing gatewayKafka Kafka consumers...")

		if err := gatewayKafka.CloseConsumers(); err != nil {
			slog.Error("Error closing gatewayKafka Kafka consumers: ", err)
		}

		slog.Info("gatewayKafka consumers are closed successfully")
	}()

	httpHandlers := handlers.NewHandlers()

	srv := server.NewServer(cfg.Server, httpHandlers)
	srv.Run()
}

