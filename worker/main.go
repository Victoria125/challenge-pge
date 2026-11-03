package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"desafio-arp/worker/internal/clickhouse"
	"desafio-arp/worker/internal/config"
	kafkaconsumer "desafio-arp/worker/internal/kafka"
	"desafio-arp/worker/internal/processor"
)

func run() error {
	cfg := config.Load()
	log.Printf("Worker iniciando: processamento ordenado, lote=%d, topico=%s",
		cfg.BatchSize, cfg.KafkaTopic)

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGINT, syscall.SIGTERM,
	)
	defer stop()

	ch, err := clickhouse.New(cfg)
	if err != nil {
		return err
	}
	defer ch.Close()

	consumer := kafkaconsumer.New(cfg)
	defer consumer.Close()

	return processor.Run(ctx, consumer, ch, processor.Config{
		BatchSize:     cfg.BatchSize,
		BatchTimeout:  time.Duration(cfg.BatchTimeoutMs) * time.Millisecond,
		RetryAttempts: cfg.RetryAttempts,
		RetryBackoff:  time.Duration(cfg.RetryBackoffMs) * time.Millisecond,
		WriteTimeout:  time.Duration(cfg.WriteTimeoutMs) * time.Millisecond,
	})
}

func main() {
	if err := run(); err != nil {
		log.Printf("Worker encerrado com erro (sem confirmar lote pendente): %v", err)
		os.Exit(1) // Docker restart: on-failure; Kafka replays uncommitted offsets.
	}
}
