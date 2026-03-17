package config

import (
	"os"
	"time"

	"github.com/notification-system-moxicom/orchestrator/internal/kafka"
	rediscfg "github.com/notification-system-moxicom/orchestrator/internal/redis"
	"github.com/notification-system-moxicom/orchestrator/internal/server"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Settings     SettingsConfig `yaml:"settings"`
	Server       server.Config  `yaml:"server"`
	Integrations Integrations   `yaml:"integrations"`
	Connections struct {
		Kafka struct {
			Gateway kafka.Config `yaml:"gateway"`
		} `yaml:"kafka"`
		Redis rediscfg.Config `yaml:"redis"`
	} `yaml:"connections"`
	Telemetry struct {
		// May add later here telemetry configs
	} `yaml:"telemetry"`
}

type SettingsConfig struct {
	ContractVersion  string            `yaml:"contract_version"`
	OperationsTopics map[string]string `yaml:"operations_topics"`
	AdapterTopics    map[string]string `yaml:"adapter_topics"`
	ReceiptTopic     string            `yaml:"receipt_topic"`
	DLQTopics        DLQTopicsConfig   `yaml:"dlq_topics"`
}

type Integrations struct {
	RPC IntegrationsRPCConfig `yaml:"rpc"`
}

type IntegrationsRPCConfig struct {
	Persistence RPCClientConfig `yaml:"persistence"`
}

type RPCClientConfig struct {
	Address        string        `yaml:"address"`
	RequestTimeout time.Duration `yaml:"request_timeout"`
}

type DLQTopicsConfig struct {
	Callbacks  string            `yaml:"callbacks"`
	Deliveries map[string]string `yaml:"deliveries"`
}

func ReadConfig(fileName string) (Config, error) {
	var cnf Config

	// nolint:gosec // we explicitly specify the path to the file
	data, err := os.ReadFile(fileName)
	if err != nil {
		return Config{}, err
	}

	err = yaml.Unmarshal(data, &cnf)
	if err != nil {
		return Config{}, err
	}

	return cnf, nil
}
