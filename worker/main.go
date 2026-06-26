package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"desafio-arp/worker/internal/clickhouse"
	"desafio-arp/worker/internal/config"
	kafkaconsumer "desafio-arp/worker/internal/kafka"
	"desafio-arp/worker/internal/model"
	"desafio-arp/worker/internal/transform"
)

type dimensoes struct {
	fornecedor model.DimFornecedor
	orgao      model.DimOrgao
	item       model.DimItem
	msg        kafka.Message
}

func main() {
	cfg := config.Load()
	log.Printf("Worker iniciando: %d goroutines, lote de %d, topico %s",
		cfg.NumWorkers, cfg.BatchSize, cfg.KafkaTopic)

	ch, err := clickhouse.New(cfg)
	if err != nil {
		log.Fatalf("ClickHouse: %v", err)
	}
	defer ch.Close()

	consumer := kafkaconsumer.New(cfg)
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	msgChan := make(chan kafka.Message, cfg.BatchSize)
	dimChan := make(chan dimensoes, cfg.BatchSize)

	var wg sync.WaitGroup

	// produtor
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(msgChan)
		for {
			msg, err := consumer.Fetch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("erro ao ler do Kafka: %v", err)
				continue
			}
			msgChan <- msg
		}
	}()

	// pool de workers
	var poolWg sync.WaitGroup
	for i := 0; i < cfg.NumWorkers; i++ {
		poolWg.Add(1)
		go func(id int) {
			defer poolWg.Done()
			for msg := range msgChan {
				var evt model.DebeziumEvent
				if err := json.Unmarshal(msg.Value, &evt); err != nil {
					log.Printf("worker %d: json invalido: %v", id, err)
					continue
				}
				if len(evt.After) == 0 || string(evt.After) == "null" {
					continue
				}
				var item model.ArpItem
				if err := json.Unmarshal(evt.After, &item); err != nil {
					log.Printf("worker %d: after invalido: %v", id, err)
					continue
				}
				transform.Higienizar(&item)
				forn, orgao, it := transform.DerivarDimensoes(item)
				dimChan <- dimensoes{forn, orgao, it, msg}
			}
		}(i)
	}

	go func() {
		poolWg.Wait()
		close(dimChan)
	}()

	agregador(ctx, cfg, ch, consumer, dimChan)

	wg.Wait()
	log.Println("Worker encerrado.")
}

func agregador(
	ctx context.Context,
	cfg config.Config,
	ch *clickhouse.Client,
	consumer *kafkaconsumer.Consumer,
	dimChan <-chan dimensoes,
) {
	ticker := time.NewTicker(time.Duration(cfg.BatchTimeoutMs) * time.Millisecond)
	defer ticker.Stop()

	var (
		forns  []model.DimFornecedor
		orgaos []model.DimOrgao
		itens  []model.DimItem
		msgs   []kafka.Message
		total  int
	)

	flush := func() {
		if len(msgs) == 0 {
			return
		}
		bgctx := context.Background()
		if err := ch.InserirFornecedores(bgctx, forns); err != nil {
			log.Printf("erro gravando fornecedores: %v", err)
		}
		if err := ch.InserirOrgaos(bgctx, orgaos); err != nil {
			log.Printf("erro gravando orgaos: %v", err)
		}
		if err := ch.InserirItens(bgctx, itens); err != nil {
			log.Printf("erro gravando itens: %v", err)
		}
		if err := consumer.Commit(bgctx, msgs...); err != nil {
			log.Printf("erro no commit de offsets: %v", err)
		}
		total += len(msgs)
		log.Printf("lote gravado: %d mensagens (total acumulado: %d)", len(msgs), total)
		forns, orgaos, itens, msgs = nil, nil, nil, nil
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case d, ok := <-dimChan:
			if !ok {
				flush()
				return
			}
			forns = append(forns, d.fornecedor)
			orgaos = append(orgaos, d.orgao)
			itens = append(itens, d.item)
			msgs = append(msgs, d.msg)
			if len(msgs) >= cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
