package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"monitoring-demo/internal/config"
	"monitoring-demo/internal/domain"
	"monitoring-demo/internal/kafka"
	"monitoring-demo/internal/storage"

	"github.com/IBM/sarama"
)

func main() {
	cfg := config.Load()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := kafka.EnsureTopic(cfg.KafkaBrokers, "dashboard-consumer", cfg.KafkaTopic, cfg.KafkaPartitions); err != nil {
		logger.Error("failed to ensure kafka topic", "error", err)
		os.Exit(1)
	}

	store, err := storage.New(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUsername, cfg.ClickHousePassword)
	if err != nil {
		logger.Error("failed to connect to clickhouse", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	group, client, err := kafka.NewConsumerGroup(cfg.KafkaBrokers, "dashboard-group", "dashboard-consumer")
	if err != nil {
		logger.Error("failed to connect to kafka", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	handler := &dashboardHandler{
		store:  store,
		logger: logger,
	}

	logger.Info("dashboard consumer started", "group", "dashboard-group", "topic", cfg.KafkaTopic)

	if err := kafka.RunConsumerGroup(ctx, group, []string{cfg.KafkaTopic}, logger, handler); err != nil && err != context.Canceled {
		logger.Error("dashboard consumer stopped", "error", err)
		os.Exit(1)
	}
}

type dashboardHandler struct {
	store  *storage.Store
	logger *slog.Logger
}

func (h *dashboardHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *dashboardHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *dashboardHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var metric domain.MetricEvent
		if err := json.Unmarshal(msg.Value, &metric); err != nil {
			h.logger.Error("skipping invalid metric payload", "partition", msg.Partition, "offset", msg.Offset, "error", err)
			session.MarkMessage(msg, "invalid payload")
			continue
		}

		if err := metric.Validate(); err != nil {
			h.logger.Error("skipping invalid metric", "partition", msg.Partition, "offset", msg.Offset, "error", err)
			session.MarkMessage(msg, "invalid metric")
			continue
		}

		ctx, cancel := context.WithTimeout(session.Context(), 5*time.Second)
		err := h.store.InsertMetric(ctx, metric)
		cancel()
		if err != nil {
			return fmt.Errorf("store metric partition=%d offset=%d: %w", msg.Partition, msg.Offset, err)
		}

		h.logger.Info(
			"stored metric",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"server_id", metric.ServerID,
			"cpu_usage", metric.CPUUsage,
			"memory_usage", metric.MemoryUsage,
			"timestamp", metric.Timestamp.UTC().Format(time.RFC3339),
		)
		session.MarkMessage(msg, "")
	}

	return nil
}
