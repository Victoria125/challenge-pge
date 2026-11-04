# Guia de demonstracao tecnica — desafio PGE-CE

Este material explica o codigo que esta no repositorio na revisao de 04/11/2026. E um guia para demonstrar o software, nao um relatorio de execucao. **Nao declare contagens, tempos, rankings ou sucesso de dashboard que nao tiver conferido no seu ambiente local.**

## Comece pelo que a pessoa consegue ver (30 segundos)

Tela: README, depois terminal.

> Meu objetivo foi construir um fluxo de dados de atas de registro de precos do Compras.gov.br: a API alimenta um PostgreSQL de stage; o Debezium envia mudancas para o Kafka; um worker em Go atualiza dimensoes no ClickHouse; o Airflow constroi e valida a fato; e o Superset consulta o resultado analitico.

Mostre o resultado de:

    docker compose ps -a

**Diferencie:** o coletor, o kafka-init e o airflow-init sao processos que terminam; **o servico Debezium deve continuar rodando para capturar novas alteracoes**. Se estiver parado, diga que novas mudancas nao estao sendo capturadas naquele momento. Nao confunda servico Debezium com conector registrado dentro do Kafka Connect.

## Parte 1 — coleta (60–90 segundos)

1. Abra **coletor/main.py**. A funcao main() instancia ComprasAPIClient e StageRepository; chama coletar_atas() e coletar_itens(); em_lotes() entrega lotes de 500; no final fecha a conexao. Se ocorrer erro esperado de API, banco ou valor, a excecao e propagada.
2. Abra **coletor/api_client.py**. _build_session() instala retries para GET em erros 429/5xx; _paginar() acrescenta pagina e tamanhoPagina, faz request com timeout, extrai resultado e para ao acabar paginasRestantes. consultar_atas() e consultar_itens() definem os dois endpoints da API.
3. Abra **coletor/transform.py**. limpar_numero() usa Decimal, sem converter dinheiro para float; limpar_booleano() trata "false" como falso; normalizar_ata() e normalizar_item() convertem os nomes/campos da API para o formato relacional.
4. Abra **coletor/repository.py**. StageRepository abre psycopg2 com autocommit=False. _deduplicar() retira chaves repetidas no lote. _upsert() executa INSERT ... ON CONFLICT ... DO UPDATE com WHERE IS DISTINCT FROM comparando os campos mutaveis; commit em sucesso, rollback em erro.
5. Abra **init-postgres/01_schema.sql**. Mostre as PKs de stage.arp_ata (numero_controle_pncp_ata) e stage.arp_item (ata, numero_item), trigger updated_at e REPLICA IDENTITY FULL para CDC.

Frase curta: **"Coleto por API, normalizo com cuidado para nao perder precisao, e so atualizo o stage quando um campo realmente mudou; assim nao gero CDC sem necessidade."**

Consulta para demonstrar dados locais:

    docker exec arp_postgres psql -U arp -d arp_stage -c "SELECT (SELECT count(*) FROM stage.arp_ata) AS atas, (SELECT count(*) FROM stage.arp_item) AS itens;"

## Parte 2 — CDC e worker Go (90–120 segundos)

1. Abra **docker-compose.yml** no trecho do PostgreSQL. O wal_level=logical habilita o mecanismo necessario ao Debezium.
2. Abra **arp-connector.json**. O conector captura as tabelas stage.arp_ata e stage.arp_item; snapshot.mode=initial; decimal.handling.mode=string. O worker, porem, consome apenas o topico da tabela de itens (arp.stage.arp_item).
3. Abra **worker/main.go**. run() carrega configuracoes, cria conexoes com ClickHouse e Kafka e chama processor.Run; se falhar, o processo sai com erro para permitir reinicio/replay.
4. Abra **worker/internal/kafka/kafka.go**. Fetch() usa FetchMessage (sem commit automatico); Commit() confirma explicitamente os offsets depois do processamento.
5. Abra **worker/internal/processor/processor.go**. decode() distingue snapshot (r), criacao (c), atualizacao (u), delete (d), tombstone e evento invalido. Run() processa **um fluxo ordenado**, sem quatro goroutines concorrentes. O lote e disparado pelo tamanho ou timeout. persist() grava fornecedores, orgaos e itens; flush() tenta salvar e so depois confirma offsets. Se falhar, o offset nao avanca.
6. Abra **worker/internal/kafka/dlq.go**. Publica eventos invalidos no topico DLQ com motivo e referencia ao offset original; o envio usa acknowledgments do broker.
7. Abra **worker/internal/transform/transform.go**. Higienizar() limpa strings; DerivarDimensoes() monta objetos de fornecedor, orgao e item.
8. Abra **worker/internal/clickhouse/clickhouse.go**. InserirFornecedores(), InserirOrgaos() e InserirItens() fazem PrepareBatch, Append e Send.
9. Abra **init-clickhouse/01_schema.sql**. As tabelas de dimensao usam ReplacingMergeTree(version), que deduplica **eventualmente**. Nao diga exactly-once; o fluxo implementa entrega **at-least-once**.

Frase curta: **"O Debezium publica as mudancas da stage no Kafka. No Go eu leio de forma ordenada, valido cada evento, gero dimensoes e persisto em lote. Confirmo offsets so depois da gravacao; mensagens malformadas vao para a DLQ."**

Atencao: a dimensao de orgaos no esquema tem campos adicionais (codigo_orgao e nome_orgao) que o worker nao preenche; dim_tempo e criada mas nao populada.

## Parte 3 — Airflow e fato (60–90 segundos)

1. Abra **airflow/dags/dag_fato_registro_precos.py**. A DAG e diaria, com max_active_runs=1, retries e tres tarefas: validar_fontes -> construir_fato -> validar_fato.
2. Mostre **validar_stage()**: confere que existem itens elegiveis (exclui item_excluido e exige chaves/valor total). validar_fontes() tambem verifica a presenca de fornecedores.
3. Mostre **montar_snapshot()**: le os dados via cursor em grupos de 1000, converte para DataFrame, chama preparar_lote() e escreve em fato_registro_precos_next. Compara contagem esperada com contagem lida/inserida e faz EXCHANGE TABLES quando esta tudo completo.
4. Abra **airflow/dags/fato_transform.py**. preparar_lote() valida identificadores, inteiros, datas e Decimals; campos financeiros invalidos geram erro, nao zeros inventados.
5. Volte a **validar_fato()**: confere contagem exata apos a troca e registra soma de valor_total.

Frase curta: **"A DAG so troca a fato depois de validar uma nova versao completa. Se a carga falhar, a fato antiga continua disponivel para as consultas."**

Nao diga que a verificacao aceita 95%: o codigo compara contagens exatas. Nao diga que a fato e atualizada com TRUNCATE direto na tabela em producao; o TRUNCATE e feito na tabela de preparacao.

## Parte 4 — Superset (40–60 segundos)

1. Mostre o painel **se ele existir e estiver abrindo normalmente na sua maquina**. Explique a origem de cada indicador por tabelas e colunas. Chame valores de "valores registrados nas atas", nao de despesas pagas.
2. Abra **docker-compose.yml**, secao superset: o servico importa um ZIP autentico se superset/exports/arp-dashboard.zip existir, e sobe o Superset conectado ao ambiente.
3. O **ZIP autentico ainda nao estava no repositorio na revisao inicial**. O processo para exportar o painel local esta descrito em superset/exports/README.md e automatizado em scripts/export_superset.sh. Nao diga que o dashboard esta versionado antes de validar a presenca do ZIP.

**Antes de recriar o container Superset da sua maquina**, exporte o dashboard local: atualmente nao ha volume de metadados persistente configurado para esse servico.

## Arquivos, um por um

| Arquivo | O que faz | Quando abrir |
|---|---|---|
| README.md | Diagrama, setup e limites conhecidos | Abertura/encerramento |
| docker-compose.yml | Rede de servicos, dependencias, ports, volumes e healthchecks | Abertura |
| arp-connector.json | Configuracao CDC para Postgres -> Kafka | CDC |
| init-postgres/01_schema.sql | Tables de stage, PK, triggers, replica identity | Stage |
| init-postgres/02_airflow_db.sql | Cria banco de metadados do Airflow | Infra |
| init-clickhouse/01_schema.sql | Dimensoes e fato analitica, engines e chaves | ClickHouse |
| coletor/.env.example | Variaveis de banco, janela e API | Setup |
| coletor/Dockerfile | Imagem Python e comando de coleta | Setup |
| coletor/requirements.txt | Requests e psycopg2 | Setup |
| coletor/config.py | Defaults da janela de coleta, timeout, credenciais e DSN | API |
| coletor/main.py | Inicia coleta de atas e itens, processa lotes, loga erros | Coleta |
| coletor/api_client.py | Pagina chamadas HTTP com retry/backoff | API |
| coletor/transform.py | Mapeia registros e valida Decimals/booleanos | Limpeza |
| coletor/repository.py | Deduplica, faz UPSERT condicional e rollback | Persistencia |
| coletor/tests/test_pipeline.py | Testes de paginacao, tipos e UPSERT | Qualidade |
| worker/Dockerfile | Build Go + imagem de execucao | Infra |
| worker/.dockerignore | Exclui arquivos do contexto do build | Infra |
| worker/go.mod | Modulo e versoes de dependencias Go | Infra |
| worker/main.go | Inicializa consumer, ClickHouse, DLQ e processor | Worker |
| worker/internal/config/config.go | Configuracoes de lote, retries e conexoes | Worker |
| worker/internal/model/model.go | Tipos dos eventos CDC e das tres dimensoes | Worker |
| worker/internal/kafka/kafka.go | Le mensagens sem auto-commit e confirma offsets | Kafka |
| worker/internal/kafka/dlq.go | Publica eventos invalidos na DLQ | Falhas |
| worker/internal/processor/processor.go | Decodifica CDC, forma lotes, persiste, confirma offsets | Principal |
| worker/internal/processor/processor_test.go | Testes de falhas, commits, deletes, DLQ e replay | Qualidade |
| worker/internal/transform/transform.go | Higieniza textos, deriva dimensoes | Transformacao |
| worker/internal/clickhouse/clickhouse.go | Abre banco analitico e grava lotes | ClickHouse |
| airflow/Dockerfile | Imagem Airflow com bibliotecas necessarias | Infra |
| airflow/requirements.txt | Dependencias Pandas, PostgreSQL e ClickHouse | Infra |
| airflow/dags/dag_fato_registro_precos.py | Valida, carrega snapshot e troca fato de modo atomico | DAG |
| airflow/dags/fato_transform.py | Valida tipos financeiros, datas e IDs da fato | Fato |
| airflow/tests/test_fato_transform.py | Testes de integridade dos dados da fato | Qualidade |
| superset/exports/README.md | Contrato de export/import e alerta sobre metadata | Superset |
| .github/workflows/go-worker-tests.yml | Testes unitarios Go no CI | Qualidade |
| .github/workflows/python-pipeline-tests.yml | Validacao Compose e testes Python no CI | Qualidade |
| .gitignore | Impede commit de .env e cache Python | Seguranca basica |
| scripts/export_superset.sh | Exporta dashboard REAL do container sem recriar | Superset |
| scripts/validate_superset_export.py | Rejeita ZIP invalido ou sem dashboard | Superset |
| scripts/tests/test_validate_superset_export.py | Testa o validador ZIP | Qualidade |

## Perguntas dificeis: responda sem esconder limite

- **"E tempo real?"** — CDC quase em tempo real para mudancas no PostgreSQL; o coletor da API nao e automaticamente reexecutado pelo Airflow.
- **"Se ClickHouse cair, perde evento?"** — o codigo usa retry e nao confirma offsets quando a persistencia falha. Ha replay; nao ha exactly-once.
- **"Por que nao quatro goroutines?"** — priorizei commits contiguos e corretos em cada particao; paralelismo particionado exige controlar a conclusao por particao.
- **"O MergeTree elimina duplicatas sempre?"** — nao imediatamente; fusoes sao eventuais e consultas criticas podem precisar de FINAL/argMax ou versoes mais robustas.
- **"A dimensao tempo esta pronta?"** — estrutura criada, carga nao implementada no repositorio; evolucao planejada.
- **"A DAG testa todas as chaves estrangeiras?"** — nao; testa stage e fornecedor, campos da fato e contagens; validacoes referenciais adicionais sao melhorias.
- **"Onde esta o dashboard versionado?"** — o export original nao foi incluido inicialmente; o processo de exportacao foi preparado, mas exige execucao na maquina que possui o painel.
- **"Onde esta o teste ponta a ponta?"** — os workflows executam testes unitarios e de validacao; nao substituem testes de integracao completos com todos os containers.

## Checklist de cinco telas para uma demonstracao curta

1. README: fluxo e objetivo.
2. Coletor: main, api_client, transform e repository.
3. Kafka/Go: arp-connector, kafka.go, processor.go e ClickHouse.
4. Airflow: DAG, preparar_lote e resultado final.
5. Superset: painel autenticado real, se existir; caso contrario explique o estado atual com transparencia.

**Fale do caminho do dado antes de falar da tecnologia.** Uma apresentacao curta com evidencias tende a ser mais defensavel do que um monologo longo.
