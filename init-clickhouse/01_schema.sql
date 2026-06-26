CREATE DATABASE IF NOT EXISTS arp_analytics;

CREATE TABLE IF NOT EXISTS arp_analytics.dim_fornecedores
(
    ni_fornecedor                 String,
    nome_razao_social_fornecedor  String,
    classificacao_fornecedor      String,
    version                       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (ni_fornecedor);

CREATE TABLE IF NOT EXISTS arp_analytics.dim_orgaos
(
    codigo_unidade_gerenciadora   Int32,
    nome_unidade_gerenciadora     String,
    codigo_orgao                  Int64,
    nome_orgao                    String,
    version                       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (codigo_unidade_gerenciadora);

CREATE TABLE IF NOT EXISTS arp_analytics.dim_itens
(
    codigo_item     Int32,
    descricao_item  String,
    tipo_item       String,
    codigo_pdm      Int32,
    nome_pdm        String,
    version         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (codigo_item);

CREATE TABLE IF NOT EXISTS arp_analytics.dim_tempo
(
    data        Date,
    ano         UInt16,
    mes         UInt8,
    trimestre   UInt8,
    nome_mes    String
)
ENGINE = MergeTree()
ORDER BY (data);

CREATE TABLE IF NOT EXISTS arp_analytics.fato_registro_precos
(
    numero_controle_pncp_ata    String,
    numero_item                 String,
    ni_fornecedor               String,
    codigo_unidade_gerenciadora Int32,
    codigo_item                 Int32,
    data_vigencia_inicial       Date,
    quantidade_homologada_item  Decimal(18,4),
    valor_unitario              Decimal(18,4),
    valor_total                 Decimal(18,2),
    version                     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (numero_controle_pncp_ata, numero_item);
