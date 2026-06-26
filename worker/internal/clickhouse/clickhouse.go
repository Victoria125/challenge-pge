package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"desafio-arp/worker/internal/config"
	"desafio-arp/worker/internal/model"
)

type Client struct {
	conn driver.Conn
}

func New(cfg config.Config) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouseURL},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouseDB,
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePass,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("abrir conexao clickhouse: %w", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) InserirFornecedores(ctx context.Context, regs []model.DimFornecedor) error {
	if len(regs) == 0 {
		return nil
	}
	batch, err := c.conn.PrepareBatch(ctx,
		"INSERT INTO dim_fornecedores (ni_fornecedor, nome_razao_social_fornecedor, classificacao_fornecedor)")
	if err != nil {
		return fmt.Errorf("preparar batch fornecedores: %w", err)
	}
	for _, r := range regs {
		if err := batch.Append(r.NiFornecedor, r.NomeRazaoSocial, r.ClassificacaoFornecedor); err != nil {
			return fmt.Errorf("append fornecedor: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InserirOrgaos(ctx context.Context, regs []model.DimOrgao) error {
	if len(regs) == 0 {
		return nil
	}
	batch, err := c.conn.PrepareBatch(ctx,
		"INSERT INTO dim_orgaos (codigo_unidade_gerenciadora, nome_unidade_gerenciadora)")
	if err != nil {
		return fmt.Errorf("preparar batch orgaos: %w", err)
	}
	for _, r := range regs {
		if err := batch.Append(r.CodigoUnidadeGerenciadora, r.NomeUnidadeGerenciadora); err != nil {
			return fmt.Errorf("append orgao: %w", err)
		}
	}
	return batch.Send()
}

func (c *Client) InserirItens(ctx context.Context, regs []model.DimItem) error {
	if len(regs) == 0 {
		return nil
	}
	batch, err := c.conn.PrepareBatch(ctx,
		"INSERT INTO dim_itens (codigo_item, descricao_item, tipo_item, codigo_pdm, nome_pdm)")
	if err != nil {
		return fmt.Errorf("preparar batch itens: %w", err)
	}
	for _, r := range regs {
		if err := batch.Append(r.CodigoItem, r.DescricaoItem, r.TipoItem, r.CodigoPdm, r.NomePdm); err != nil {
			return fmt.Errorf("append item: %w", err)
		}
	}
	return batch.Send()
}
