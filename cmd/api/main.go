package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"monitoring-demo/internal/config"
	"monitoring-demo/internal/domain"
	"monitoring-demo/internal/kafka"

	"github.com/IBM/sarama"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := kafka.EnsureTopic(cfg.KafkaBrokers, "producer-api", cfg.KafkaTopic, cfg.KafkaPartitions); err != nil {
		logger.Error("failed to ensure kafka topic", "error", err)
		os.Exit(1)
	}

	producer, err := kafka.NewSyncProducer(cfg.KafkaBrokers, "producer-api")
	if err != nil {
		logger.Error("failed to connect to kafka", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("failed to close producer:", "error", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", handleMetrics(producer, cfg.KafkaTopic, logger))

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("producer api listening", "addr", cfg.HTTPAddr, "topic", cfg.KafkaTopic)

	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server failed", "error", err)
		os.Exit(1)
	}
}

func handleMetrics(producer sarama.SyncProducer, topic string, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		defer func() {
			if err := r.Body.Close(); err != nil {
				logger.Error("failed to close body:", "error", err)
			}
		}()
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		var metric domain.MetricEvent
		if err := decoder.Decode(&metric); err != nil {
			http.Error(w, fmt.Sprintf("invalid json payload: %v", err), http.StatusBadRequest)
			return
		}

		if err := metric.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		body, err := json.Marshal(metric)
		if err != nil {
			http.Error(w, "failed to serialize metric", http.StatusInternalServerError)
			return
		}

		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(metric.ServerID),
			Value: sarama.ByteEncoder(body),
		})
		if err != nil {
			logger.Error("failed to publish metric", "error", err, "server_id", metric.ServerID)
			http.Error(w, "failed to publish metric", http.StatusServiceUnavailable)
			return
		}

		logger.Info("published metric", "partition", partition, "offset", offset, "server_id", metric.ServerID)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "accepted",
			"partition": partition,
			"offset":    offset,
		})
	}
}
