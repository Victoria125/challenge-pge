// Package config centraliza a configuracao do worker, lida de variaveis
// de ambiente para funcionar bem dentro do docker-compose.
package config

import (
	"os"
	"strconv"
)

type Config struct {
	KafkaBroker   string
	KafkaTopic    string
	KafkaGroupID  string
	ClickHouseURL string
	ClickHouseDB  string
	ClickHouseUser string
	ClickHousePass string
	NumWorkers    int
	BatchSize     int
	BatchTimeoutMs int
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
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
		ClickHouseURL:  getenv("CLICKHOUSE_URL", "clickhouse:9000"),
		ClickHouseDB:   getenv("CLICKHOUSE_DB", "arp_analytics"),
		ClickHouseUser: getenv("CLICKHOUSE_USER", "arp"),
		ClickHousePass: getenv("CLICKHOUSE_PASS", "arp"),
		// pool de goroutines que processam mensagens em paralelo
		NumWorkers: getenvInt("NUM_WORKERS", 4),
		// quantos registros acumular antes de gravar em lote no ClickHouse
		BatchSize: getenvInt("BATCH_SIZE", 1000),
		// tempo maximo de espera antes de gravar um lote parcial (ms)
		BatchTimeoutMs: getenvInt("BATCH_TIMEOUT_MS", 2000),
	}
}
