package model

import "encoding/json"

type DebeziumEvent struct {
	After json.RawMessage `json:"after"`
	Op    string          `json:"op"`
}

type ArpItem struct {
	NumeroControlePncpAta     string `json:"numero_controle_pncp_ata"`
	NumeroItem                string `json:"numero_item"`
	CodigoUnidadeGerenciadora int32  `json:"codigo_unidade_gerenciadora"`
	NomeUnidadeGerenciadora   string `json:"nome_unidade_gerenciadora"`
	CodigoItem                int32  `json:"codigo_item"`
	DescricaoItem             string `json:"descricao_item"`
	TipoItem                  string `json:"tipo_item"`
	CodigoPdm                 int32  `json:"codigo_pdm"`
	NomePdm                   string `json:"nome_pdm"`
	NiFornecedor              string `json:"ni_fornecedor"`
	NomeRazaoSocialFornecedor string `json:"nome_razao_social_fornecedor"`
	ClassificacaoFornecedor   string `json:"classificacao_fornecedor"`
	ItemExcluido              bool   `json:"item_excluido"`
}

type DimFornecedor struct {
	NiFornecedor            string
	NomeRazaoSocial         string
	ClassificacaoFornecedor string
}

type DimOrgao struct {
	CodigoUnidadeGerenciadora int32
	NomeUnidadeGerenciadora   string
}

type DimItem struct {
	CodigoItem    int32
	DescricaoItem string
	TipoItem      string
	CodigoPdm     int32
	NomePdm       string
}
