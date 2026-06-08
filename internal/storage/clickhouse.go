package storage

import (
	"context"
	"time"

	"monitoring-demo/internal/domain"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Store struct {
	conn clickhouse.Conn
}

func New(ctx context.Context, addr, database, username, password string) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &Store{conn: conn}, nil
}

func (s *Store) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func (s *Store) InsertMetric(ctx context.Context, metric domain.MetricEvent) error {
	return s.conn.Exec(ctx,
		`INSERT INTO metrics_dashboard (server_id, cpu_usage, memory_usage, timestamp) VALUES (?, ?, ?, ?)`,
		metric.ServerID,
		metric.CPUUsage,
		metric.MemoryUsage,
		metric.Timestamp.UTC(),
	)
}

func (s *Store) InsertAlerts(ctx context.Context, alerts []domain.AlertEvent) error {
	if len(alerts) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `INSERT INTO detected_alerts (server_id, alert_type, metric_value, event_timestamp, detected_at)`)
	if err != nil {
		return err
	}

	for _, alert := range alerts {
		if err := batch.Append(
			alert.ServerID,
			alert.AlertType,
			alert.MetricValue,
			alert.EventTimestamp.UTC(),
			alert.DetectedAt.UTC(),
		); err != nil {
			return err
		}
	}

	return batch.Send()
}

func (s *Store) InsertAlert(ctx context.Context, alert domain.AlertEvent) error {
	return s.conn.Exec(ctx,
		`INSERT INTO detected_alerts (server_id, alert_type, metric_value, event_timestamp, detected_at) VALUES (?, ?, ?, ?, ?)`,
		alert.ServerID,
		alert.AlertType,
		alert.MetricValue,
		alert.EventTimestamp.UTC(),
		alert.DetectedAt.UTC(),
	)
}
