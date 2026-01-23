package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/notification-system-moxicom/orchestrator/api"
)

const (
	applicationJSONContentType = "application/json"
	readHeaderTimeout          = 0
	shutdownTimeout            = 30 * time.Second
)

type Config struct {
	HTTP struct {
		Address string `yaml:"address"`
	} `yaml:"http"`
}

type HTTPHandlers interface {
	api.ServerInterface
}
type Server struct {
	httpServer *http.Server
	handlers   HTTPHandlers
}

func NewServer(h HTTPHandlers) *Server {
	return &Server{
		handlers: h,
	}
}

func (s *Server) AddHTTPServer(c Config) {
	corsOptions := cors.Options{
		AllowedOrigins:   []string{"https://*", "http://*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	}

	mux := chi.NewRouter()
	mux.Use(middleware.NoCache)
	mux.Use(middleware.SetHeader("Content-Type", applicationJSONContentType))
	mux.Use(cors.Handler(corsOptions))

	mux.Route("/api/api-gateway/v1", func(r chi.Router) {
		r.Mount("/", api.Handler(s.handlers)) // TODO: fixme. replace nil with s.handlers
	})

	s.httpServer = &http.Server{
		Handler:           mux,
		Addr:              c.HTTP.Address,
		ReadHeaderTimeout: readHeaderTimeout,
	}
}

func (s *Server) Run() {
	// Create a channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	// Start HTTP server in a goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
			} else {
			}
		}
	}()

	// Block until we receive a signal
	<-stop
	slog.Info("Shutdown signal received, initiating HTTP server graceful shutdown...")

	// Create a context with timeout for shutdown
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Shutdown HTTP server
	if err := s.httpServer.Shutdown(ctx); err != nil {
		slog.Error("HTTP server shutdown error: ", err)
	} else {
		// s.logger.Info("HTTP server shutdown complete")
	}
}
