# Arquitetura Inicial

## Visao Geral

O projeto segue uma arquitetura Lakehouse com padrao Medallion, separando responsabilidades por camada e priorizando reprocessamento, observabilidade e expansao futura para ambientes como Azure Databricks.

O dominio adotado nesta etapa e vendas, com foco em clientes, produtos, pedidos e itens de pedido.

## Decisoes ja aplicadas

- `src/lakehouse` concentra toda a logica da aplicacao.
- `storage/` representa o data lake local por camadas.
- `configs/base.yml` centraliza defaults nao sensiveis.
- `.env` sobrescreve parametros por ambiente.
- `infra/docker-compose.yml` sobe a infraestrutura local inicial.
- `scripts/sync_sales_sources.py` sincroniza a API publica, os CSVs e o PostgreSQL usando o mesmo snapshot.

## Componentes da primeira entrega

- API publica DummyJSON para snapshot de vendas.
- PostgreSQL como fonte transacional local no schema `sales`.
- exportacoes CSV derivadas do mesmo conjunto de dados.
- Spark com Delta configurado no codigo da aplicacao.
- logging padronizado para jobs.
- estrutura pronta para evoluir para Landing, Bronze, Silver e Gold.

## Componentes da evolucao atual

- `storage/landing` guarda o snapshot bruto e os CSVs do batch.
- `storage/bronze/{source}/{entity}` passa a materializar Delta por fonte.
- a Bronze reaproveita o mesmo lote de vendas para API, CSV e PostgreSQL.
- `src/lakehouse/quality/rules.py` centraliza regras iniciais de qualidade.
- `src/lakehouse/quality/gates.py` traduz `quality_results` persistidos em decisao operacional.
- `src/lakehouse/silver/sales.py` prepara datasets Silver limpos e resultados de validacao.
- `src/lakehouse/silver/service.py` persiste os datasets Silver e o pacote de qualidade em disco.
- `src/lakehouse/gold/sales.py` agrega visoes analiticas de vendas por dia, mes, cliente e produto.
- `src/lakehouse/gold/service.py` constroi a Gold a partir da Silver persistida e materializa marts em disco.
- `src/lakehouse/orchestration/sales_pipeline.py` define o contrato reutilizavel da orquestracao.
- `dags/sales_medallion_pipeline.py` expoe a DAG lida pelo Airflow.
- `scripts/check_sales_quality.py` bloqueia promocoes quando a Silver falha no gate.
- `scripts/alert_sales_quality.py` gera o resumo operacional consumido pelo Airflow em caso de falha.
- `infra/airflow/Dockerfile` prepara a imagem do orquestrador com Java e dependencias do projeto.
