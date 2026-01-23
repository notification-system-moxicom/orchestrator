package main

import (
	"flag"
	"log/slog"

	"github.com/notification-system-moxicom/orchestrator/internal/config"
	"github.com/notification-system-moxicom/orchestrator/internal/http/handlers"
	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	"github.com/notification-system-moxicom/orchestrator/internal/server"
	"github.com/notification-system-moxicom/orchestrator/internal/validation"
	"github.com/notification-system-moxicom/orchestrator/pkg/logger"
)

func main() {
	var configPath string

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

	orchestratorKafka, err := kafka.NewService(&cfg.Connections.Kafka.CamundaCore, validator)
	if err != nil {
		slog.Error("failed to create Kafka service:", slog.String("error", err.Error()))
		return
	}

	defer func() {
		slog.Info("Closing orchestratorKafka Kafka producer...")

		if err := orchestratorKafka.CloseProducer(); err != nil {
			slog.Error("Error closing orchestratorKafka Kafka producer: ", err)
		}

		slog.Info("orchestratorKafka producer closed successfully")
	}()

	err = orchestratorKafka.Produce("test_topic", []byte(`{"test_key":"test_value"}`))
	if err != nil {
		slog.Error("failed to produce test message:", slog.String("error", err.Error()))
		return
	}

	httpHandlers := handlers.NewHandlers()

	server.NewServer(httpHandlers)
}
