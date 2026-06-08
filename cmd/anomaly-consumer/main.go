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

	if err := kafka.EnsureTopic(cfg.KafkaBrokers, "anomaly-consumer", cfg.KafkaTopic, cfg.KafkaPartitions); err != nil {
		logger.Error("failed to ensure kafka topic", "error", err)
		os.Exit(1)
	}

	store, err := storage.New(ctx, cfg.ClickHouseAddr, cfg.ClickHouseDatabase, cfg.ClickHouseUsername, cfg.ClickHousePassword)
	if err != nil {
		logger.Error("failed to connect to clickhouse", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	group, client, err := kafka.NewConsumerGroup(cfg.KafkaBrokers, "anomaly-group", "anomaly-consumer")
	if err != nil {
		logger.Error("failed to connect to kafka", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	handler := &anomalyHandler{
		store:  store,
		client: client,
		logger: logger,
		topic:  cfg.KafkaTopic,
	}

	logger.Info("anomaly consumer started", "group", "anomaly-group", "topic", cfg.KafkaTopic)

	if err := kafka.RunConsumerGroup(ctx, group, []string{cfg.KafkaTopic}, logger, handler); err != nil && err != context.Canceled {
		logger.Error("anomaly consumer stopped", "error", err)
		os.Exit(1)
	}
}

type anomalyHandler struct {
	store              *storage.Store
	client             sarama.Client
	logger             *slog.Logger
	topic              string
	snapshotEndOffsets map[int32]int64
}

func (h *anomalyHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.snapshotEndOffsets = make(map[int32]int64)

	partitions := session.Claims()[h.topic]
	for _, partition := range partitions {
		offset, err := h.client.GetOffset(h.topic, partition, sarama.OffsetNewest)
		if err != nil {
			return fmt.Errorf("fetch latest offset for partition %d: %w", partition, err)
		}
		h.snapshotEndOffsets[partition] = offset
	}

	h.logger.Info("captured replay offsets", "topic", h.topic, "offsets", h.snapshotEndOffsets)
	return nil
}

func (h *anomalyHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *anomalyHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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

		source := "live"
		if snapshot, ok := h.snapshotEndOffsets[msg.Partition]; ok && msg.Offset < snapshot {
			source = "historical_replay"
		}

		h.logger.Info(
			"processed metric",
			"partition", msg.Partition,
			"offset", msg.Offset,
			"server_id", metric.ServerID,
			"source", source,
		)

		alerts := make([]domain.AlertEvent, 0, 2)
		if metric.CPUUsage > 90 {
			alerts = append(alerts, domain.NewAlertEvent(metric, domain.AlertHighCPU, metric.CPUUsage, time.Now().UTC()))
		}
		if metric.MemoryUsage > 90 {
			alerts = append(alerts, domain.NewAlertEvent(metric, domain.AlertHighMemory, metric.MemoryUsage, time.Now().UTC()))
		}

		if len(alerts) > 0 {
			ctx, cancel := context.WithTimeout(session.Context(), 5*time.Second)
			err := h.store.InsertAlerts(ctx, alerts)
			cancel()
			if err != nil {
				return fmt.Errorf("store alerts partition=%d offset=%d: %w", msg.Partition, msg.Offset, err)
			}

			for _, alert := range alerts {
				h.logger.Warn(
					"detected anomaly",
					"partition", msg.Partition,
					"offset", msg.Offset,
					"server_id", alert.ServerID,
					"alert_type", alert.AlertType,
					"metric_value", alert.MetricValue,
					"source", source,
				)
			}
		}

		session.MarkMessage(msg, "")
	}

	return nil
}
