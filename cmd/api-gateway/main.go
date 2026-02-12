package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/notification-system-moxicom/orchestrator/internal/config"
	"github.com/notification-system-moxicom/orchestrator/internal/http/handlers"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka/handler/gateway"
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
