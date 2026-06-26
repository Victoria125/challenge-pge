package transform

import (
	"strings"

	"desafio-arp/worker/internal/model"
)

func limparTexto(s string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(s)), " ")
}

func Higienizar(item *model.ArpItem) {
	item.NomeUnidadeGerenciadora = limparTexto(item.NomeUnidadeGerenciadora)
	item.DescricaoItem = limparTexto(item.DescricaoItem)
	item.TipoItem = limparTexto(item.TipoItem)
	item.NomePdm = limparTexto(item.NomePdm)
	item.NiFornecedor = limparTexto(item.NiFornecedor)
	item.NomeRazaoSocialFornecedor = limparTexto(item.NomeRazaoSocialFornecedor)
	item.ClassificacaoFornecedor = limparTexto(item.ClassificacaoFornecedor)
}

func DerivarDimensoes(item model.ArpItem) (model.DimFornecedor, model.DimOrgao, model.DimItem) {
	forn := model.DimFornecedor{
		NiFornecedor:            item.NiFornecedor,
		NomeRazaoSocial:         item.NomeRazaoSocialFornecedor,
		ClassificacaoFornecedor: item.ClassificacaoFornecedor,
	}
	orgao := model.DimOrgao{
		CodigoUnidadeGerenciadora: item.CodigoUnidadeGerenciadora,
		NomeUnidadeGerenciadora:   item.NomeUnidadeGerenciadora,
	}
	it := model.DimItem{
		CodigoItem:    item.CodigoItem,
		DescricaoItem: item.DescricaoItem,
		TipoItem:      item.TipoItem,
		CodigoPdm:     item.CodigoPdm,
		NomePdm:       item.NomePdm,
	}
	return forn, orgao, it
}
