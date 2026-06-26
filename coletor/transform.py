from decimal import Decimal, InvalidOperation
from typing import Any, Optional


def limpar_numero(valor: Any) -> Optional[float]:
    """Converte valor para float tratando notacao cientifica (ex: 0E-8)."""
    if valor is None:
        return None
    try:
        return float(Decimal(str(valor)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def limpar_texto(valor: Any) -> Optional[str]:
    """Remove espacos nas pontas; string vazia vira None."""
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


def limpar_inteiro(valor: Any) -> Optional[int]:
    if valor is None or valor == "":
        return None
    try:
        return int(valor)
    except (ValueError, TypeError):
        return None


def normalizar_ata(reg: dict) -> dict:
    return {
        "numero_controle_pncp_ata": limpar_texto(reg.get("numeroControlePncpAta")),
        "numero_ata_registro_preco": limpar_texto(reg.get("numeroAtaRegistroPreco")),
        "codigo_unidade_gerenciadora": limpar_texto(reg.get("codigoUnidadeGerenciadora")),
        "nome_unidade_gerenciadora": limpar_texto(reg.get("nomeUnidadeGerenciadora")),
        "codigo_orgao": limpar_inteiro(reg.get("codigoOrgao")),
        "nome_orgao": limpar_texto(reg.get("nomeOrgao")),
        "numero_compra": limpar_texto(reg.get("numeroCompra")),
        "ano_compra": limpar_texto(reg.get("anoCompra")),
        "codigo_modalidade_compra": limpar_texto(reg.get("codigoModalidadeCompra")),
        "nome_modalidade_compra": limpar_texto(reg.get("nomeModalidadeCompra")),
        "objeto": limpar_texto(reg.get("objeto")),
        "valor_total": limpar_numero(reg.get("valorTotal")),
        "status_ata": limpar_texto(reg.get("statusAta")),
        "quantidade_itens": limpar_inteiro(reg.get("quantidadeItens")),
        "data_assinatura": limpar_texto(reg.get("dataAssinatura")),
        "data_vigencia_inicial": limpar_texto(reg.get("dataVigenciaInicial")),
        "data_vigencia_final": limpar_texto(reg.get("dataVigenciaFinal")),
        "link_ata_pncp": limpar_texto(reg.get("linkAtaPNCP")),
        "link_compra_pncp": limpar_texto(reg.get("linkCompraPNCP")),
        "numero_controle_pncp_compra": limpar_texto(reg.get("numeroControlePncpCompra")),
        "id_compra": limpar_texto(reg.get("idCompra")),
        "ata_excluido": bool(reg.get("ataExcluido", False)),
        "data_hora_exclusao_fonte": limpar_texto(reg.get("dataHoraExclusao")),
        "data_hora_inclusao_fonte": limpar_texto(reg.get("dataHoraInclusao")),
        "data_hora_atualizacao_fonte": limpar_texto(reg.get("dataHoraAtualizacao")),
    }


def normalizar_item(reg: dict) -> dict:
    return {
        "numero_controle_pncp_ata": limpar_texto(reg.get("numeroControlePncpAta")),
        "numero_item": limpar_texto(reg.get("numeroItem")),
        "numero_ata_registro_preco": limpar_texto(reg.get("numeroAtaRegistroPreco")),
        "codigo_unidade_gerenciadora": limpar_inteiro(reg.get("codigoUnidadeGerenciadora")),
        "nome_unidade_gerenciadora": limpar_texto(reg.get("nomeUnidadeGerenciadora")),
        "numero_compra": limpar_texto(reg.get("numeroCompra")),
        "ano_compra": limpar_texto(reg.get("anoCompra")),
        "codigo_modalidade_compra": limpar_texto(reg.get("codigoModalidadeCompra")),
        "nome_modalidade_compra": limpar_texto(reg.get("nomeModalidadeCompra")),
        "codigo_item": limpar_inteiro(reg.get("codigoItem")),
        "descricao_item": limpar_texto(reg.get("descricaoItem")),
        "tipo_item": limpar_texto(reg.get("tipoItem")),
        "codigo_pdm": limpar_inteiro(reg.get("codigoPdm")),
        "nome_pdm": limpar_texto(reg.get("nomePdm")),
        "quantidade_homologada_item": limpar_numero(reg.get("quantidadeHomologadaItem")),
        "ni_fornecedor": limpar_texto(reg.get("niFornecedor")),
        "nome_razao_social_fornecedor": limpar_texto(reg.get("nomeRazaoSocialFornecedor")),
        "classificacao_fornecedor": limpar_texto(reg.get("classificacaoFornecedor")),
        "valor_unitario": limpar_numero(reg.get("valorUnitario")),
        "valor_total": limpar_numero(reg.get("valorTotal")),
        "quantidade_homologada_vencedor": limpar_numero(reg.get("quantidadeHomologadaVencedor")),
        "quantidade_empenhada": limpar_numero(reg.get("quantidadeEmpenhada")),
        "maximo_adesao": limpar_numero(reg.get("maximoAdesao")),
        "percentual_maior_desconto": limpar_numero(reg.get("percentualMaiorDesconto")),
        "data_assinatura": limpar_texto(reg.get("dataAssinatura")),
        "data_vigencia_inicial": limpar_texto(reg.get("dataVigenciaInicial")),
        "data_vigencia_final": limpar_texto(reg.get("dataVigenciaFinal")),
        "situacao_sicaf": limpar_texto(reg.get("situacaoSicaf")),
        "id_compra": limpar_texto(reg.get("idCompra")),
        "numero_controle_pncp_compra": limpar_texto(reg.get("numeroControlePncpCompra")),
        "item_excluido": bool(reg.get("itemExcluido", False)),
        "data_hora_exclusao_fonte": limpar_texto(reg.get("dataHoraExclusao")),
        "data_hora_inclusao_fonte": limpar_texto(reg.get("dataHoraInclusao")),
        "data_hora_atualizacao_fonte": limpar_texto(reg.get("dataHoraAtualizacao")),
    }
