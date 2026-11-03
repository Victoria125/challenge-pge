package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"desafio-arp/worker/internal/model"
)

type fakeConsumer struct {
	messages    []Message
	next        int
	commitCalls int
	committed   []Message
	failCommits int
	order       *[]string
}

func (f *fakeConsumer) Fetch(context.Context) (Message, error) {
	if f.next >= len(f.messages) {
		return Message{}, io.EOF
	}
	m := f.messages[f.next]
	f.next++
	return m, nil
}

func (f *fakeConsumer) Commit(_ context.Context, messages ...Message) error {
	f.commitCalls++
	*f.order = append(*f.order, "commit")
	if f.failCommits > 0 {
		f.failCommits--
		return errors.New("Kafka indisponivel")
	}
	f.committed = append(f.committed, messages...)
	return nil
}

type fakeDLQ struct {
	order *[]string
	fail int
	published []Message
}

func (f *fakeDLQ) Publish(_ context.Context, msg Message, _ error) error {
	*f.order = append(*f.order, "dlq")
	if f.fail > 0 {
		f.fail--
		return errors.New("DLQ indisponivel")
	}
	f.published = append(f.published, msg)
	return nil
}

type fakeSink struct {
	order          *[]string
	failFornecedores int
	failOrgaos       int
	failItens        int
}

func (f *fakeSink) InserirFornecedores(_ context.Context, _ []model.DimFornecedor) error {
	*f.order = append(*f.order, "fornecedores")
	if f.failFornecedores > 0 {
		f.failFornecedores--
		return errors.New("ClickHouse fornecedores indisponivel")
	}
	return nil
}

func (f *fakeSink) InserirOrgaos(_ context.Context, _ []model.DimOrgao) error {
	*f.order = append(*f.order, "orgaos")
	if f.failOrgaos > 0 {
		f.failOrgaos--
		return errors.New("ClickHouse orgaos indisponivel")
	}
	return nil
}

func (f *fakeSink) InserirItens(_ context.Context, _ []model.DimItem) error {
	*f.order = append(*f.order, "itens")
	if f.failItens > 0 {
		f.failItens--
		return errors.New("ClickHouse itens indisponivel")
	}
	return nil
}

func configForTest(size int) Config {
	return Config{
		BatchSize: size, BatchTimeout: time.Second,
		RetryAttempts: 2, RetryBackoff: time.Microsecond,
		WriteTimeout: time.Second,
	}
}

func event(itemNum int) []byte {
	raw, err := json.Marshal(map[string]any{
		"numero_controle_pncp_ata": "ATA-1",
		"numero_item": fmt.Sprint(itemNum),
		"ni_fornecedor": "12345678901234",
		"codigo_item": itemNum,
	})
	if err != nil {
		panic(err)
	}
	data, err := json.Marshal(model.DebeziumEvent{
		Op: "c", After: raw,
	})
	if err != nil {
		panic(err)
	}
	return data
}

func message(offset int64, value []byte) Message {
	return Message{Topic: "arp.stage.arp_item", Partition: 0, Offset: offset, Value: value}
}

func TestBatchIsPersistedBeforeCommit(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{
		message(10, event(1)), message(11, event(2)),
	}}
	sink := &fakeSink{order: &order}
	err := Run(context.Background(), reader, sink, &fakeDLQ{order: &order}, configForTest(2))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("esperava fim do reader fake, recebeu: %v", err)
	}
	expected := []string{"fornecedores", "orgaos", "itens", "commit"}
	if !reflect.DeepEqual(order, expected) {
		t.Fatalf("ordem inesperada: %v", order)
	}
	if len(reader.committed) != 2 || reader.committed[1].Offset != 11 {
		t.Fatalf("offsets nao confirmados corretamente: %#v", reader.committed)
	}
}

func TestFailedClickHouseWriteNeverCommits(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{message(10, event(1))}}
	sink := &fakeSink{order: &order, failOrgaos: 2}
	err := Run(context.Background(), reader, sink, &fakeDLQ{order: &order}, configForTest(1))
	if err == nil || !strings.Contains(err.Error(), "offsets preservados") {
		t.Fatalf("esperava falha permanente de gravacao, recebeu: %v", err)
	}
	if reader.commitCalls != 0 {
		t.Fatalf("confirmou %d vezes mesmo com falha", reader.commitCalls)
	}
}

func TestTransientClickHouseFailureIsRetried(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{message(10, event(1))}}
	sink := &fakeSink{order: &order, failFornecedores: 1}
	err := Run(context.Background(), reader, sink, &fakeDLQ{order: &order}, configForTest(1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("esperava concluir lote antes de EOF: %v", err)
	}
	expected := []string{"fornecedores", "fornecedores", "orgaos", "itens", "commit"}
	if !reflect.DeepEqual(order, expected) {
		t.Fatalf("retry/commit fora de ordem: %v", order)
	}
}

func TestCommitRetryDoesNotRepeatInserts(t *testing.T) {
	var order []string
	reader := &fakeConsumer{
		order: &order, messages: []Message{message(10, event(1))},
		failCommits: 1,
	}
	sink := &fakeSink{order: &order}
	err := Run(context.Background(), reader, sink, &fakeDLQ{order: &order}, configForTest(1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("erro inesperado: %v", err)
	}
	expected := []string{"fornecedores", "orgaos", "itens", "commit", "commit"}
	if !reflect.DeepEqual(order, expected) {
		t.Fatalf("commit deve ser refeito sem reinserir: %v", order)
	}
}

func TestInvalidCDCIsDLQPublishedBeforeCommit(t *testing.T) {
	for _, v := range [][]byte{
		[]byte("{invalido"),
		[]byte("{\"op\":\"u\",\"after\":null}"),
		[]byte("{\"op\":\"unknown\",\"after\":{}}"),
	} {
		var order []string
		reader := &fakeConsumer{order: &order, messages: []Message{message(17, v)}}
		sink := &fakeSink{order: &order}
		dlq := &fakeDLQ{order: &order}
		err := Run(context.Background(), reader, sink, dlq, configForTest(1))
		if !errors.Is(err, io.EOF) {
			t.Fatalf("evento invalido deve ir para DLQ e continuar: %v", err)
		}
		if len(dlq.published) != 1 || reader.commitCalls != 1 {
			t.Fatalf("DLQ/offset inconsistente: events=%d commits=%d", len(dlq.published), reader.commitCalls)
		}
		if len(order) == 0 || order[0] != "dlq" || order[len(order)-1] != "commit" {
			t.Fatalf("DLQ deve preceder o commit: %v", order)
		}
	}
}

func TestUnavailableDLQPreventsOffsetCommit(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{message(17, []byte("{invalido"))}}
	dlq := &fakeDLQ{order: &order, fail: 2}
	err := Run(context.Background(), reader, &fakeSink{order: &order}, dlq, configForTest(1))
	if err == nil || !strings.Contains(err.Error(), "offset nao confirmado") {
		t.Fatalf("DLQ indisponivel deve falhar sem commit: %v", err)
	}
	if reader.commitCalls != 0 {
		t.Fatal("DLQ indisponivel jamais confirma offset")
	}
}

func TestCDCDeleteAdvancesWithoutRemovingDimensions(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{
		message(18, []byte("{\"op\":\"d\",\"after\":null}")),
	}}
	sink := &fakeSink{order: &order}
	dlq := &fakeDLQ{order: &order}
	if err := Run(context.Background(), reader, sink, dlq, configForTest(1)); !errors.Is(err, io.EOF) {
		t.Fatalf("delete esperado como evento valido sem insercao: %v", err)
	}
	if reader.commitCalls != 1 || len(dlq.published) != 0 {
		t.Fatalf("delete deve ser confirmado sem DLQ: %#v", order)
	}
}

func TestExcludedItemStillPersistsDimensions(t *testing.T) {
	var order []string
	raw := []byte("{\"op\":\"r\",\"after\":{\"numero_controle_pncp_ata\":\"ATA-1\",\"numero_item\":\"1\",\"codigo_item\":1,\"item_excluido\":true}}")
	reader := &fakeConsumer{order: &order, messages: []Message{message(19, raw)}}
	if err := Run(context.Background(), reader, &fakeSink{order: &order},
		&fakeDLQ{order: &order}, configForTest(1)); !errors.Is(err, io.EOF) {
		t.Fatalf("item excluido deve manter dimensoes: %v", err)
	}
	expected := []string{"fornecedores", "orgaos", "itens", "commit"}
	if !reflect.DeepEqual(order, expected) {
		t.Fatalf("item excluido deixou de processar dimensoes: %v", order)
	}
}

func TestInvalidBetweenValidRowsDoesNotSkipEarlierOffsets(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{
		message(0, event(1)),
		message(1, []byte("{invalid")),
		message(2, event(2)),
	}}
	dlq := &fakeDLQ{order: &order}
	if err := Run(context.Background(), reader, &fakeSink{order: &order},
		dlq, configForTest(3)); !errors.Is(err, io.EOF) {
		t.Fatalf("deveria continuar após DLQ: %v", err)
	}
	if reader.commitCalls != 1 || len(reader.committed) != 3 || len(dlq.published) != 1 {
		t.Fatalf("ordem/perda de offsets: committed=%d, dlq=%d",
			len(reader.committed), len(dlq.published))
	}
}
 
func TestTombstoneIsAcknowledgedWithoutInserts(t *testing.T) {
	var order []string
	reader := &fakeConsumer{order: &order, messages: []Message{message(18, nil)}}
	sink := &fakeSink{order: &order}
	err := Run(context.Background(), reader, sink, &fakeDLQ{order: &order}, configForTest(1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("erro inesperado: %v", err)
	}
	// Even when empty, the sink methods are called; no rows are written by the real sink.
	if reader.commitCalls != 1 || len(reader.committed) != 1 {
		t.Fatalf("tombstone deve ser confirmada: %#v", reader.committed)
	}
}
