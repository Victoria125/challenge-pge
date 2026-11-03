// Package config centraliza as configuracoes do worker.
package config

import (
	"os"
	"strconv"
)

type Config struct {
	KafkaBroker     string
	KafkaTopic      string
	KafkaGroupID    string
	KafkaDLQTopic   string
	ClickHouseURL   string
	ClickHouseDB    string
	ClickHouseUser  string
	ClickHousePass  string
	BatchSize       int
	BatchTimeoutMs  int
	RetryAttempts   int
	RetryBackoffMs  int
	WriteTimeoutMs  int
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func Load() Config {
	return Config{
		KafkaBroker:    getenv("KAFKA_BROKER", "kafka:29092"),
		KafkaTopic:     getenv("KAFKA_TOPIC", "arp.stage.arp_item"),
		KafkaGroupID:   getenv("KAFKA_GROUP_ID", "arp-worker-go"),
		KafkaDLQTopic: getenv("KAFKA_DLQ_TOPIC", "arp.stage.arp_item.dlq"),
		ClickHouseURL:  getenv("CLICKHOUSE_URL", "clickhouse:9000"),
		ClickHouseDB:   getenv("CLICKHOUSE_DB", "arp_analytics"),
		ClickHouseUser: getenv("CLICKHOUSE_USER", "arp"),
		ClickHousePass: getenv("CLICKHOUSE_PASS", "arp"),
		BatchSize:      getenvInt("BATCH_SIZE", 1000),
		BatchTimeoutMs: getenvInt("BATCH_TIMEOUT_MS", 2000),
		RetryAttempts:  getenvInt("RETRY_ATTEMPTS", 3),
		RetryBackoffMs: getenvInt("RETRY_BACKOFF_MS", 1000),
		WriteTimeoutMs: getenvInt("WRITE_TIMEOUT_MS", 30000),
	}
}
