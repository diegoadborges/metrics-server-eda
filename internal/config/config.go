package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr           string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaPartitions    int32
	ClickHouseAddr     string
	ClickHouseDatabase string
	ClickHouseUsername string
	ClickHousePassword string
}

func Load() Config {
	return Config{
		HTTPAddr:           envOrDefault("HTTP_ADDR", ":8080"),
		KafkaBrokers:       splitAndTrim(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         envOrDefault("KAFKA_TOPIC", "server-metrics"),
		KafkaPartitions:    int32(envIntOrDefault("KAFKA_TOPIC_PARTITIONS", 3)),
		ClickHouseAddr:     envOrDefault("CLICKHOUSE_ADDR", "localhost:9000"),
		ClickHouseDatabase: envOrDefault("CLICKHOUSE_DATABASE", "default"),
		ClickHouseUsername: envOrDefault("CLICKHOUSE_USER", "default"),
		ClickHousePassword: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
