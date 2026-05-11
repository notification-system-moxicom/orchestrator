package main

import (
	"context"
	"flag"
	"log/slog"
	"time"

	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/config"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/http/handlers"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/kafka"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/kafka/handler/delivery"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/kafka/handler/gateway"
	persistencev1 "gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/persistence"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/server"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/validation"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/pkg/logger"
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
		"notification_message": "schemas/send_notification.json",
	}
	validator, err := validation.NewJSONSchemaMessageValidator(schemaFiles)
	if err != nil {
		slog.Error("failed to create JSON schema validator:", slog.String("error", err.Error()))
		return
	}

	gatewayKafka, err := kafka.NewService(&cfg.Connections.Kafka.Gateway, validator)
	if err != nil {
		slog.Error("failed to create Kafka service:", slog.String("error", err.Error()))
		return
	}

	apiGatewayHandler := gateway.NewHanler(gatewayKafka, cfg.Settings.OperationsTopics)
	apiGatewayHandler.StartConsumer(ctx, cfg.Connections.Kafka.Gateway.ConsumerWorkersCount)

	// Persistence gRPC client for delivery status updates
	persistenceClient, err := persistencev1.NewDeliveryStatusClient(
		cfg.Integrations.RPC.Persistence.Address,
		5*time.Second,
	)
	if err != nil {
		slog.Error("failed to create persistence client:", slog.String("error", err.Error()))
		return
	}

	defer func() {
		if err := persistenceClient.Close(); err != nil {
			slog.Error("Error closing persistence client: ", err)
		}
	}()

	// Delivery status callback handler
	deliveryHandler := delivery.NewHandler(gatewayKafka, persistenceClient, cfg.Settings.OperationsTopics)
	deliveryHandler.StartConsumer(ctx, cfg.Connections.Kafka.Gateway.ConsumerWorkersCount)

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
