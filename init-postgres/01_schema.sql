-- =============================================================================
-- Provisionamento automatico do PostgreSQL (area de Stage - Parte 1)
-- Executado na primeira inicializacao do container.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS stage;

-- -----------------------------------------------------------------------------
-- Tabela de cabecalho das Atas de Registro de Preco
-- Fonte: GET /modulo-arp/1_consultarARP
-- Granularidade: 1 linha por ata.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.arp_ata (
    numero_controle_pncp_ata     TEXT NOT NULL,

    numero_ata_registro_preco    TEXT,
    codigo_unidade_gerenciadora  TEXT,
    nome_unidade_gerenciadora    TEXT,
    codigo_orgao                 BIGINT,
    nome_orgao                   TEXT,
    numero_compra                TEXT,
    ano_compra                   TEXT,
    codigo_modalidade_compra     TEXT,
    nome_modalidade_compra       TEXT,
    objeto                       TEXT,
    valor_total                  NUMERIC(18,2),
    status_ata                   TEXT,
    quantidade_itens             INTEGER,

    data_assinatura              DATE,
    data_vigencia_inicial        DATE,
    data_vigencia_final          DATE,

    link_ata_pncp                TEXT,
    link_compra_pncp             TEXT,
    numero_controle_pncp_compra  TEXT,
    id_compra                    TEXT,

    -- soft delete vindo da fonte
    ata_excluido                 BOOLEAN DEFAULT FALSE,
    data_hora_exclusao_fonte     TIMESTAMP,

    -- timestamps da fonte (API)
    data_hora_inclusao_fonte     TIMESTAMP,
    data_hora_atualizacao_fonte  TIMESTAMP,

    -- timestamps de auditoria do robo (controle de CDC e idempotencia)
    ingested_at                  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                   TIMESTAMP NOT NULL DEFAULT now(),

    PRIMARY KEY (numero_controle_pncp_ata)
);

-- -----------------------------------------------------------------------------
-- Tabela de itens das Atas (grao fino - base para dimensoes e fato)
-- Fonte: GET /modulo-arp/2_consultarARPItem
-- Granularidade: 1 linha por item de ata.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stage.arp_item (
    numero_controle_pncp_ata        TEXT NOT NULL,
    numero_item                     TEXT NOT NULL,

    -- atributos herdados do cabecalho da ata
    numero_ata_registro_preco       TEXT,
    codigo_unidade_gerenciadora     INTEGER,
    nome_unidade_gerenciadora       TEXT,
    numero_compra                   TEXT,
    ano_compra                      TEXT,
    codigo_modalidade_compra        TEXT,
    nome_modalidade_compra          TEXT,

    -- atributos do item (dim_itens)
    codigo_item                     INTEGER,
    descricao_item                  TEXT,
    tipo_item                       TEXT,
    codigo_pdm                      INTEGER,
    nome_pdm                        TEXT,
    quantidade_homologada_item      NUMERIC(18,4),

    -- fornecedor (dim_fornecedores)
    ni_fornecedor                   TEXT,
    nome_razao_social_fornecedor    TEXT,
    classificacao_fornecedor        TEXT,

    -- metricas (fato_registro_precos)
    valor_unitario                  NUMERIC(18,4),
    valor_total                     NUMERIC(18,2),
    quantidade_homologada_vencedor  NUMERIC(18,4),
    quantidade_empenhada            NUMERIC(18,4),
    maximo_adesao                   NUMERIC(18,4),
    percentual_maior_desconto       NUMERIC(9,4),

    -- vigencia
    data_assinatura                 TIMESTAMP,
    data_vigencia_inicial           DATE,
    data_vigencia_final             DATE,

    situacao_sicaf                  TEXT,
    id_compra                       TEXT,
    numero_controle_pncp_compra     TEXT,

    -- soft delete
    item_excluido                   BOOLEAN DEFAULT FALSE,
    data_hora_exclusao_fonte        TIMESTAMP,

    -- timestamps da fonte
    data_hora_inclusao_fonte        TIMESTAMP,
    data_hora_atualizacao_fonte     TIMESTAMP,

    -- auditoria do robo
    ingested_at                     TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                      TIMESTAMP NOT NULL DEFAULT now(),

    PRIMARY KEY (numero_controle_pncp_ata, numero_item)
);

-- Indices uteis para a coleta incremental e joins posteriores
CREATE INDEX IF NOT EXISTS idx_arp_item_atualizacao
    ON stage.arp_item (data_hora_atualizacao_fonte);
CREATE INDEX IF NOT EXISTS idx_arp_item_fornecedor
    ON stage.arp_item (ni_fornecedor);
CREATE INDEX IF NOT EXISTS idx_arp_ata_atualizacao
    ON stage.arp_ata (data_hora_atualizacao_fonte);

-- -----------------------------------------------------------------------------
-- Trigger para manter updated_at coerente a cada UPSERT.
-- E o updated_at que o Debezium observa para emitir eventos de UPDATE.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION stage.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_arp_ata_updated_at
    BEFORE UPDATE ON stage.arp_ata
    FOR EACH ROW EXECUTE FUNCTION stage.set_updated_at();

CREATE TRIGGER trg_arp_item_updated_at
    BEFORE UPDATE ON stage.arp_item
    FOR EACH ROW EXECUTE FUNCTION stage.set_updated_at();

-- REPLICA IDENTITY FULL: faz o Debezium enviar o estado completo da linha
-- (antes e depois) nos eventos de UPDATE/DELETE. Util para o worker Go.
ALTER TABLE stage.arp_ata REPLICA IDENTITY FULL;
ALTER TABLE stage.arp_item REPLICA IDENTITY FULL;
