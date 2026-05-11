package config

import (
	"os"

	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/kafka"
	"gitlab.services.mts.ru/nm/notification-system/orchestrator/internal/server"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Settings     SettingsConfig `yaml:"settings"`
	Server       server.Config  `yaml:"server"`
	Integrations Integrations   `yaml:"integrations"`
	Connections  struct {
		Kafka struct {
			Gateway kafka.Config `yaml:"gateway"`
		} `yaml:"kafka"`
	} `yaml:"connections"`
	Telemetry struct {
		// May add later here telemetry configs
	} `yaml:"telemetry"`
}

type SettingsConfig struct {
	ContractVersion  string            `yaml:"contract_version"`
	OperationsTopics map[string]string `yaml:"operations_topics"`
}

type Integrations struct {
	RPC IntegrationsRPCConfig `yaml:"rpc"`
}

type IntegrationsRPCConfig struct {
	Persistence struct {
		Address string `yaml:"address"`
	} `yaml:"persistence"`
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
