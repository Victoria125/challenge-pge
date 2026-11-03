package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"desafio-arp/worker/internal/config"
	"desafio-arp/worker/internal/processor"
)

// DLQPublisher uses synchronous, broker-acknowledged Kafka writes.
type DLQPublisher struct {
	writer *kafka.Writer
}

func NewDLQPublisher(cfg config.Config) *DLQPublisher {
	return &DLQPublisher{writer: &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBroker),
		Topic:        cfg.KafkaDLQTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}}
}

type deadLetter struct {
	SourceTopic string    `json:"source_topic"`
	Partition   int       `json:"partition"`
	Offset      int64     `json:"offset"`
	Reason      string    `json:"reason"`
	Original    []byte    `json:"original_value_base64"`
	FailedAt    time.Time `json:"failed_at"`
}

func (p *DLQPublisher) Publish(ctx context.Context, msg processor.Message, cause error) error {
	value, err := json.Marshal(deadLetter{
		SourceTopic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset,
		Reason: cause.Error(), Original: msg.Value, FailedAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("serializar DLQ: %w", err)
	}
	key := fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(key), Value: value,
	}); err != nil {
		return fmt.Errorf("publicar evento na DLQ: %w", err)
	}
	return nil
}

func (p *DLQPublisher) Close() error {
	return p.writer.Close()
}
