package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"

	"desafio-arp/worker/internal/config"
	"desafio-arp/worker/internal/processor"
)

type Consumer struct {
	reader *kafka.Reader
}

func New(cfg config.Config) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.KafkaBroker},
		Topic:       cfg.KafkaTopic,
		GroupID:     cfg.KafkaGroupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    10e3,
		MaxBytes:    10e6,
	})
	return &Consumer{reader: reader}
}

// FetchMessage never commits offsets automatically.
func (c *Consumer) Fetch(ctx context.Context) (processor.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return processor.Message{}, err
	}
	return processor.Message{
		Topic: msg.Topic, Partition: msg.Partition,
		Offset: msg.Offset, Value: msg.Value,
	}, nil
}

// CommitMessages advances through the highest offset in each partition.
// The processor passes ONLY contiguous, already persisted messages.
func (c *Consumer) Commit(ctx context.Context, msgs ...processor.Message) error {
	commits := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		commits[i] = kafka.Message{
			Topic: msg.Topic, Partition: msg.Partition,
			Offset: msg.Offset,
		}
	}
	return c.reader.CommitMessages(ctx, commits...)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
