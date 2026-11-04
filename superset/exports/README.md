# Exportacao e reproducibilidade do Superset

**Estado atual:** o repositorio ainda nao inclui o ZIP do dashboard original. Nenhum grafico ou valor foi inventado. A importacao automatica no docker-compose ja existe, mas depende de um export real em superset/exports/arp-dashboard.zip.

## 1. Salvar o dashboard QUE JA EXISTE no seu Superset

**Faca isso ANTES de recriar/remover o container atual.** A configuracao atual nao monta um volume persistente para o banco de metadados do Superset; um container recriado pode perder o dashboard feito pela interface. Nao execute docker compose down -v.

Execute na raiz do projeto, no computador onde arp_superset esta rodando:

    bash scripts/export_superset.sh

O script:
- verifica se o container existente esta rodando;
- executa superset export_dashboards --dashboard-file no container;
- copia o ZIP para o repositorio, sem reiniciar o servidor;
- valida integridade e presenca de pelo menos um dashboard YAML;
- faz backup com data/hora de um export anterior antes de substitui-lo;
- **nao** envia nada automaticamente ao GitHub.

Para conferir o conteudo:

    python3 scripts/validate_superset_export.py superset/exports/arp-dashboard.zip
    unzip -l superset/exports/arp-dashboard.zip

Confira os YAMLs internos antes de publicar, especialmente URLs locais, nomes de fontes, configuracoes e quaisquer campos confidenciais. O repositorio e publico. O export pode conter configuracoes de conexao; verifique se nao ha segredos. Os arquivos .backup.* sao copias locais, nao devem ser commitados.

**Se o script retornar "Export sem dashboards"**, significa que o container consultado nao continha um dashboard salvo; nao substitua um export valido por um ZIP vazio. Abra a interface do Superset e verifique se o painel original ainda existe.

## 2. Tornar a parte 4 reproduzivel

Depois de revisar o ZIP para remover segredos:

    git add superset/exports/arp-dashboard.zip
    git commit -m "feat(superset): include authentic dashboard export"
    git push

Em instalacoes novas, o docker-compose.yml importa automaticamente arp-dashboard.zip se ele existir no caminho acima. O bootstrap atual usa:

    superset import_dashboards --path /app/superset-import/arp-dashboard.zip --username admin

O import carrega os metadados exportados, nao os dados do ClickHouse. O banco analitico precisa estar preenchido, e a conexao do dataset exportado precisa apontar para clickhouse:8123, com credenciais validas para o ambiente. Senhas de conexao podem precisar ser reconfiguradas manualmente por seguranca.

Para importar em um Superset ja inicializado, sem apagar o ambiente:

    docker exec arp_superset superset import_dashboards --path /app/superset-import/arp-dashboard.zip --username admin

**Atencao:** o import usa sobrescrita para ativos com UUID correspondente. Nao importe sobre um dashboard alterado sem backup. Para confirmar a reproducibilidade, teste o ZIP em uma instalacao de desenvolvimento separada, nao na instancia unica de demonstracao.

## 3. Limitacoes e proximos passos

- O ZIP autentico so pode ser obtido no computador/instancia que possui o dashboard. Apenas alterar o GitHub nao permite extrair dados de um container remoto privado.
- Esta alteracao fornece um processo seguro para exportar, validar e importar, mas **nao cria um painel ausente**.
- O metadata DB do Superset atualmente vive dentro do container. Depois de exportar e validar o painel original, planeje migrar esses metadados para armazenamento persistente (um volume ou banco dedicado). **Nao adicione um volume em cima do caminho existente sem primeiro fazer backup**, pois isso pode ocultar os dados atuais.
- O Superset deste projeto e um ambiente de desenvolvimento com credenciais de demonstracao. Antes de qualquer uso institucional, revise secrets, acesso e versoes de dependencias.
