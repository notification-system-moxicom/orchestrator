package logger

import (
	"log/slog"
	"os"
)

type Config struct {
	Level string `yaml:"level"`
}

func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.MessageKey {
		a.Key = ".msg"
	}

	if a.Key == slog.TimeKey {
		a.Key = ".time"
	}

	if a.Key == slog.LevelKey {
		a.Key = ".level"
	}

	return a
}

func SetDefaults(cfg *Config) {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	})

	slog.SetDefault(slog.New(handler))
}
