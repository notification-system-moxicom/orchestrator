package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Address              string        `yaml:"address"`
	Password             string        `yaml:"password"`
	DB                   int           `yaml:"db"`
	PoolSize             int           `yaml:"pool_size"`
	ScenarioPollInterval time.Duration `yaml:"scenario_poll_interval"`
}

func NewClient(cfg Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", cfg.Address, err)
	}

	return rdb, nil
}
