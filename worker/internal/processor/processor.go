// Package processor implements ordered, at-least-once CDC processing.
// Kafka offsets advance only after every ClickHouse insert in a batch succeeds.
package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"desafio-arp/worker/internal/model"
	"desafio-arp/worker/internal/transform"
)

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Value     []byte
}

type Consumer interface {
	Fetch(context.Context) (Message, error)
	Commit(context.Context, ...Message) error
}

type Sink interface {
	InserirFornecedores(context.Context, []model.DimFornecedor) error
	InserirOrgaos(context.Context, []model.DimOrgao) error
	InserirItens(context.Context, []model.DimItem) error
}

type Config struct {
	BatchSize     int
	BatchTimeout  time.Duration
	RetryAttempts int
	RetryBackoff  time.Duration
	WriteTimeout  time.Duration
}

type batch struct {
	messages    []Message
	fornecedores []model.DimFornecedor
	orgaos      []model.DimOrgao
	itens       []model.DimItem
}

func (b *batch) add(msg Message, item *model.ArpItem) {
	b.messages = append(b.messages, msg)
	if item == nil {
		return // Kafka tombstone: no row to insert, but it can be acknowledged.
	}
	fornecedor, orgao, it := transform.DerivarDimensoes(*item)
	b.fornecedores = append(b.fornecedores, fornecedor)
	b.orgaos = append(b.orgaos, orgao)
	b.itens = append(b.itens, it)
}

// decode rejects unsupported deletes and malformed records rather than
// acknowledging them as if they had been applied to ClickHouse.
func decode(value []byte) (*model.ArpItem, error) {
	payload := bytes.TrimSpace(value)
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		return nil, nil // Debezium Kafka tombstone (distinct from a delete event).
	}

	var evt model.DebeziumEvent
	if err := json.Unmarshal(payload, &evt); err != nil {
		return nil, fmt.Errorf("JSON de CDC invalido: %w", err)
	}
	switch evt.Op {
	case "c", "u", "r":
		// create, update, initial snapshot
	case "d":
		return nil, errors.New("DELETE de CDC ainda nao suportado: evento nao confirmado")
	default:
		return nil, fmt.Errorf("operacao CDC desconhecida %q", evt.Op)
	}
	if len(evt.After) == 0 || bytes.Equal(bytes.TrimSpace(evt.After), []byte("null")) {
		return nil, errors.New("CDC sem after valido")
	}

	var item model.ArpItem
	if err := json.Unmarshal(evt.After, &item); err != nil {
		return nil, fmt.Errorf("after invalido: %w", err)
	}
	if item.NumeroControlePncpAta == "" || item.NumeroItem == "" {
		return nil, errors.New("CDC sem chave da ata ou do item")
	}
	if item.ItemExcluido {
		return nil, errors.New("item marcado como excluido: remocao na dimensao nao implementada")
	}
	transform.Higienizar(&item)
	return &item, nil
}

// Run fetches in a single ordered stream. A future parallel version must
// preserve contiguous per-partition offset completion before each commit.
func Run(ctx context.Context, consumer Consumer, sink Sink, cfg Config) error {
	if cfg.BatchSize <= 0 || cfg.BatchTimeout <= 0 ||
		cfg.RetryAttempts <= 0 || cfg.RetryBackoff <= 0 || cfg.WriteTimeout <= 0 {
		return errors.New("configuracao de batch, timeout ou retry invalida")
	}

	var pending batch
	var firstReceived time.Time

	for {
		fetchCtx := ctx
		cancel := func() {}
		if len(pending.messages) > 0 {
			fetchCtx, cancel = context.WithDeadline(ctx, firstReceived.Add(cfg.BatchTimeout))
		}

		msg, err := consumer.Fetch(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				// No offsets advanced: unfinished work is replayed after restart.
				log.Printf("interrompido; %d eventos pendentes sem commit", len(pending.messages))
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) && len(pending.messages) > 0 {
				if err := flush(ctx, consumer, sink, &pending, cfg); err != nil {
					return err
				}
				pending = batch{}
				continue
			}
			return fmt.Errorf("falha ao ler Kafka: %w", err)
		}

		item, err := decode(msg.Value)
		if err != nil {
			return fmt.Errorf("CDC partition=%d offset=%d: %w", msg.Partition, msg.Offset, err)
		}
		if len(pending.messages) == 0 {
			firstReceived = time.Now()
		}
		pending.add(msg, item)
		if len(pending.messages) >= cfg.BatchSize {
			if err := flush(ctx, consumer, sink, &pending, cfg); err != nil {
				return err
			}
			pending = batch{}
		}
	}
}

func persist(ctx context.Context, sink Sink, b *batch) error {
	if err := sink.InserirFornecedores(ctx, b.fornecedores); err != nil {
		return fmt.Errorf("gravar fornecedores: %w", err)
	}
	if err := sink.InserirOrgaos(ctx, b.orgaos); err != nil {
		return fmt.Errorf("gravar orgaos: %w", err)
	}
	if err := sink.InserirItens(ctx, b.itens); err != nil {
		return fmt.Errorf("gravar itens: %w", err)
	}
	return nil
}

func retryWait(ctx context.Context, base time.Duration, attempt int) error {
	wait := base
	for i := 1; i < attempt && wait < 30*time.Second; i++ {
		wait *= 2
	}
	if wait > 30*time.Second {
		wait = 30 * time.Second
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func flush(ctx context.Context, consumer Consumer, sink Sink, b *batch, cfg Config) error {
	if len(b.messages) == 0 {
		return nil
	}

	// Every stage is re-runnable. A retry may write the first dimensions twice;
	// ReplacingMergeTree collapses identical keys eventually, not atomically.
	saved := false
	for attempt := 1; attempt <= cfg.RetryAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(ctx, cfg.WriteTimeout)
		err := persist(writeCtx, sink, b)
		cancel()
		if err == nil {
			saved = true
			break
		}
		log.Printf("falha persistindo lote (%d/%d): %v", attempt, cfg.RetryAttempts, err)
		if attempt == cfg.RetryAttempts {
			return fmt.Errorf("lote nao persistido; offsets preservados: %w", err)
		}
		if err := retryWait(ctx, cfg.RetryBackoff, attempt); err != nil {
			return err
		}
	}
	if !saved {
		return errors.New("lote nao persistido")
	}

	// Retry the COMMIT itself without re-inserting the batch.
	for attempt := 1; attempt <= cfg.RetryAttempts; attempt++ {
		commitCtx, cancel := context.WithTimeout(ctx, cfg.WriteTimeout)
		err := consumer.Commit(commitCtx, b.messages...)
		cancel()
		if err == nil {
			log.Printf("lote persistido e confirmado: %d eventos", len(b.messages))
			return nil
		}
		log.Printf("falha commit Kafka (%d/%d): %v", attempt, cfg.RetryAttempts, err)
		if attempt == cfg.RetryAttempts {
			return fmt.Errorf("dados salvos mas commit nao confirmado; replay possivel: %w", err)
		}
		if err := retryWait(ctx, cfg.RetryBackoff, attempt); err != nil {
			return err
		}
	}
	return errors.New("commit nao confirmado")
}
