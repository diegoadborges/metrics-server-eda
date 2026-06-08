package domain

import (
	"fmt"
	"strings"
	"time"
)

const (
	AlertHighCPU    = "HIGH_CPU"
	AlertHighMemory = "HIGH_MEMORY"
)

type MetricEvent struct {
	ServerID    string    `json:"server_id"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryUsage float64   `json:"memory_usage"`
	Timestamp   time.Time `json:"timestamp"`
}

func (m MetricEvent) Validate() error {
	if strings.TrimSpace(m.ServerID) == "" {
		return fmt.Errorf("server_id is required")
	}
	if m.CPUUsage < 0 || m.CPUUsage > 100 {
		return fmt.Errorf("cpu_usage must be between 0 and 100")
	}
	if m.MemoryUsage < 0 || m.MemoryUsage > 100 {
		return fmt.Errorf("memory_usage must be between 0 and 100")
	}
	if m.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	return nil
}

type AlertEvent struct {
	ServerID       string    `json:"server_id"`
	AlertType      string    `json:"alert_type"`
	MetricValue    float64   `json:"metric_value"`
	EventTimestamp time.Time `json:"event_timestamp"`
	DetectedAt     time.Time `json:"detected_at"`
}

func NewAlertEvent(metric MetricEvent, alertType string, metricValue float64, detectedAt time.Time) AlertEvent {
	return AlertEvent{
		ServerID:       metric.ServerID,
		AlertType:      alertType,
		MetricValue:    metricValue,
		EventTimestamp: metric.Timestamp.UTC(),
		DetectedAt:     detectedAt.UTC(),
	}
}
