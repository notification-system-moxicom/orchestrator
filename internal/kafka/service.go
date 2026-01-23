package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	"github.com/xdg-go/scram"

	"github.com/notification-system-moxicom/orchestrator/internal/errors"
	"github.com/notification-system-moxicom/orchestrator/internal/validation"
	"github.com/notification-system-moxicom/orchestrator/pkg/util/generic"
)

const (
	defaultRetryTimeout      = 5 * time.Second
	defaultDialTimeout       = 5 * time.Second
	defaultRetryMax          = 3
	defaultInitialBackoff    = 1 * time.Second
	defaultMaxBackoff        = 60 * time.Second
	defaultBackoffMultiplier = 2.0
	defaultMaxReconnectTries = 0 // 0 means unlimited

	StartProcessConsumer        = "start-process"
	CreateServiceTaskConsumer   = "create-service-task-consumer"
	FileUploadResultConsumer    = "file-upload-result-consumer"
	RequestActionsConsumer      = "request-actions-consumer"
	FallbackConsumer            = "fallback-consumer"
	DelegatesResponseConsumer   = "delegates-response-consumer"
	CreateUserTaskConsumer      = "create-user-task-consumer"
	GroupLoginsResponseConsumer = "group-logins-response-consumer"
)

type Config struct {
	Brokers                []string      `yaml:"brokers"`
	ConsumerGroup          string        `yaml:"consumer_group"`
	ProducerTopics         []string      `yaml:"producer_topics"`
	Retry                  RetryConfig   `yaml:"retry"`
	HealthCheckTimeout     time.Duration `yaml:"health_check_timeout"`
	ConsumerWorkersCount   int           `yaml:"consumer_workers_count"`
	ProducerFlushBytes     int           `yaml:"producer_flush_bytes"`
	ProducerFlushMessages  int           `yaml:"producer_flush_messages"`
	ProducerFlushFrequency time.Duration `yaml:"producer_flush_frequency"`
	Auth                   AuthConfig    `yaml:"auth"`
}

type AuthConfig struct {
	EnableAuth  bool   `yaml:"enable_auth"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	Certificate string `yaml:"certificate"`
	Mechanism   string `yaml:"mechanism"`
}

type RetryConfig struct {
	Timeout           time.Duration `yaml:"net_timeout"`
	MaxCount          int           `yaml:"max_retries"`
	DialTimeout       time.Duration `yaml:"dial_timeout"`
	InitialBackoff    time.Duration `yaml:"initial_backoff"`
	MaxBackoff        time.Duration `yaml:"max_backoff"`
	BackoffMultiplier float64       `yaml:"backoff_multiplier"`
	MaxReconnectTries int           `yaml:"max_reconnect_tries"`
}

type MessageHandler interface {
	HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
}

type Service struct {
	serviceConfig *Config

	producer  sarama.SyncProducer
	consumers map[string]sarama.ConsumerGroup

	validator *validation.JSONSchemaMessageValidator
	wg        sync.WaitGroup
}

func NewService(
	cfg *Config,
	validator *validation.JSONSchemaMessageValidator,
) (*Service, error) {
	if cfg == nil {
		return nil, errors.NewConfigurationError("kafka config is nil", nil)
	}

	if cfg.HealthCheckTimeout == 0 {
		return nil, errors.NewConfigurationError(
			"field health_check isn't specified in the config",
			nil,
		)
	}

	cfg.Retry.Timeout = generic.DefaultIfZero(cfg.Retry.Timeout, defaultRetryTimeout)
	cfg.Retry.DialTimeout = generic.DefaultIfZero(cfg.Retry.DialTimeout, defaultDialTimeout)
	cfg.Retry.MaxCount = generic.DefaultIfZero(cfg.Retry.MaxCount, defaultRetryMax)
	cfg.Retry.InitialBackoff = generic.DefaultIfZero(cfg.Retry.InitialBackoff, defaultInitialBackoff)
	cfg.Retry.MaxBackoff = generic.DefaultIfZero(cfg.Retry.MaxBackoff, defaultMaxBackoff)
	cfg.Retry.BackoffMultiplier = generic.DefaultIfZero(cfg.Retry.BackoffMultiplier, defaultBackoffMultiplier)
	cfg.Retry.MaxReconnectTries = generic.DefaultIfZero(cfg.Retry.MaxReconnectTries, defaultMaxReconnectTries)

	producer, err := NewProducer(cfg, &cfg.Auth, &cfg.Retry)
	if err != nil {
		return nil, err
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Net.DialTimeout = cfg.Retry.DialTimeout
	saramaCfg.Metadata.Retry.Max = cfg.Retry.MaxCount
	saramaCfg.Metadata.Retry.Backoff = cfg.Retry.Timeout
	saramaCfg.Net.MaxOpenRequests = 1
	saramaCfg.Version = sarama.V2_8_0_0

	if cfg.Auth.EnableAuth {
		err = configureAuth(&cfg.Auth, saramaCfg)
		if err != nil {
			return nil, err
		}
	}

	consumers := make(map[string]sarama.ConsumerGroup)
	//
	// EXAMPLE OF ADDING CONSUMER GROUP
	//
	//startProcessConsumer, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, saramaCfg)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to create consumer group: %w", err)
	//}
	//
	//createServiceTaskConsumer, err := sarama.NewConsumerGroup(
	//	cfg.Brokers,
	//	cfg.ConsumerGroup,
	//	saramaCfg,
	//)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to create consumer group: %w", err)
	//}

	//consumers[StartProcessConsumer] = startProcessConsumer

	return &Service{
		serviceConfig: cfg,
		producer:      producer,
		consumers:     consumers,
		validator:     validator,
	}, nil
}

func NewProducer(cfg *Config, auth *AuthConfig, retry *RetryConfig) (sarama.SyncProducer, error) {
	m := metrics.DefaultRegistry
	m.UnregisterAll()

	saramaCfg := sarama.NewConfig()

	if auth != nil && auth.EnableAuth {
		if err := configureAuth(auth, saramaCfg); err != nil {
			return nil, err
		}
	}

	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Idempotent = false
	saramaCfg.Net.DialTimeout = retry.DialTimeout
	saramaCfg.Metadata.Retry.Max = retry.MaxCount
	saramaCfg.Metadata.Retry.Backoff = retry.Timeout

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	return producer, nil
}

func configureAuth(auth *AuthConfig, saramaCfg *sarama.Config) error {
	tlsConfig, err := createTLSConfig(os.Getenv(auth.Certificate))
	if err != nil {
		return err
	}

	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = tlsConfig

	passwordDecoded, err := base64.StdEncoding.DecodeString(os.Getenv(auth.Password))
	if err != nil {
		return err
	}

	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.User = os.Getenv(auth.Username)
	saramaCfg.Net.SASL.Password = string(passwordDecoded)
	saramaCfg.Net.SASL.Mechanism = sarama.SASLMechanism(auth.Mechanism)
	saramaCfg.Net.SASL.Handshake = true
	saramaCfg.Net.SASL.Version = sarama.SASLHandshakeV1

	saramaCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
		return &xdgScramClient{
			HashGeneratorFcn: scram.SHA512,
		}
	}

	return nil
}

func createTLSConfig(caBase64 string) (*tls.Config, error) {
	caCertPEMDecoded, err := base64.StdEncoding.DecodeString(caBase64)
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEMDecoded)

	return &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
		MinVersion:         tls.VersionTLS12,
	}, nil
}

func (s *Service) Produce(topic string, msg any) error {
	if s == nil {
		return errors.NewKafkaError("kafka service is unavailable", nil)
	}

	//err := s.validator.Validate(msg)
	//if err != nil {
	//	errMsg := "JSON validation failed for " + fmt.Sprintf("%T", msg) + err.Error()
	//
	//	return fmt.Errorf("%s", errMsg)
	//}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(jsonBytes),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to kafka: %w", err)
	}

	return nil
}

// calculateBackoff calculates the backoff duration with jitter for reconnection attempts
func calculateBackoff(attempt int, initialBackoff, maxBackoff time.Duration, multiplier float64) time.Duration {
	// Exponential backoff
	backoff := float64(initialBackoff) * math.Pow(multiplier, float64(attempt))

	if backoff > float64(maxBackoff) {
		backoff = float64(maxBackoff)
	}

	// Jitter to avoid thundering herd
	// nolint:gosec // unimportant G404: Use of weak random number generator (math/rand instead of crypto/rand)
	jitter := rand.Float64()*0.4 - 0.2 // +-20%
	backoff *= (1 + jitter)

	return time.Duration(backoff)
}

func (s *Service) StartConsumer(
	ctx context.Context,
	consumerKey string,
	topics []string,
	handler MessageHandler,
	workersCount int,
) {
	if s == nil {
		return
	}

	var (
		cg sarama.ConsumerGroup
		ok bool
	)

	if cg, ok = s.consumers[consumerKey]; !ok {
		slog.Error("consumer not found: ", consumerKey)
		return
	}

	s.wg.Add(1)

	// nolint:nestif // it's ok
	go func() {
		defer s.wg.Done()

		var attempt int

		for {
			// Check if service is being closed or context is done
			select {
			case <-ctx.Done():
				slog.Info("context canceled, stopping consumer ", consumerKey)
				return
			default:
				// Continue execution
			}

			consumerHandler := NewConsumerHandler(handler, workersCount)
			err := cg.Consume(ctx, topics, consumerHandler)

			// Check if service is being closed or context is done
			select {
			case <-ctx.Done():
				slog.Info("context canceled, stopping consumer ", consumerKey)
				return
			default:
				// Continue execution
			}

			// Handle error with exponential backoff
			if err != nil {
				attempt++

				// Check if we've reached the maximum number of reconnect tries
				if s.serviceConfig.Retry.MaxReconnectTries > 0 && attempt > s.serviceConfig.Retry.MaxReconnectTries {
					slog.Error("maximum reconnection attempts reached (", s.serviceConfig.Retry.MaxReconnectTries,
						") for consumer ", consumerKey, ", stopping")

					return
				}

				backoff := calculateBackoff(
					attempt,
					s.serviceConfig.Retry.InitialBackoff,
					s.serviceConfig.Retry.MaxBackoff,
					s.serviceConfig.Retry.BackoffMultiplier,
				)

				reconnectInfo := "reconnecting in " + backoff.String() + " seconds (attempt " + strconv.Itoa(attempt) + ")"
				slog.Warn("error from consumer ", consumerKey, err, "reconnect", reconnectInfo)
				// Wait for backoff duration or until context is canceled or service is closed
				select {
				case <-time.After(backoff):
					// Continue with reconnect
				case <-ctx.Done():
					slog.Info("context canceled during backoff, stopping consumer ", consumerKey)
					return
				}
			} else {
				attempt = 0

				slog.Info("consumer ", consumerKey, " rebalanced or returned without error, reconnecting")
			}
		}
	}()
}

// WaitForConsumers waits for all consumer goroutines to finish with timeout
func (s *Service) WaitForConsumers(ctx context.Context) {
	if s == nil {
		return
	}

	slog.Info("Waiting for all Kafka consumers to finish...")

	// Create a channel to signal when wg.Wait() completes
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	// Wait for either all goroutines to finish or context timeout
	select {
	case <-done:
		slog.Info("All Kafka consumers finished gracefully")
	case <-ctx.Done():
		slog.Error("Timed out waiting for Kafka consumers to finish")
	}
}

// CloseConsumers gracefully shuts down the Kafka consumers
func (s *Service) CloseConsumers() error {
	if s == nil {
		return nil
	}

	// Close all consumers
	for key, consumer := range s.consumers {
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				slog.Error("error closing Kafka consumer ", key, ": ", err)
				// Continue with closing other consumers even if one fails
			} else {
				slog.Info("Kafka consumer ", key, ", closed successfully")
			}
		}
	}

	return nil
}

func (s *Service) CloseProducer() error {
	if s == nil {
		return nil
	}

	// Then close the producer
	if s.producer != nil {
		if err := s.producer.Close(); err != nil {
			slog.Error("error closing Kafka producer: %v", err)
			return err
		}
	}

	return nil
}
