# Arquitetura Inicial

## Visão Geral

O projeto segue uma arquitetura Lakehouse com padrão Medallion, separando responsabilidades por camada e priorizando reprocessamento, observabilidade e expansão futura para ambientes como Azure Databricks.

O domínio adotado nesta etapa é vendas, com foco em clientes, produtos, pedidos e itens de pedido.

## Decisões já aplicadas

- `src/lakehouse` concentra toda a lógica da aplicação.
- `storage/` representa o data lake local por camadas.
- `configs/base.yml` centraliza defaults não sensíveis.
- `.env` sobrescreve parâmetros por ambiente.
- `infra/docker-compose.yml` sobe a infraestrutura local inicial.
- `scripts/sync_sales_sources.py` sincroniza a API pública, os CSVs e o PostgreSQL usando o mesmo snapshot.

## Componentes da primeira entrega

- API pública DummyJSON para snapshot de vendas.
- PostgreSQL como fonte transacional local no schema `sales`.
- exportações CSV derivadas do mesmo conjunto de dados.
- Spark com Delta configurado no código da aplicação.
- logging padronizado para jobs.
- estrutura pronta para evoluir para Landing, Bronze, Silver e Gold.

## Componentes da evolução atual

- `storage/landing` guarda o snapshot bruto e os CSVs do batch.
- `storage/bronze/{source}/{entity}` passa a materializar Delta por fonte.
- a Bronze reaproveita o mesmo lote de vendas para API, CSV e PostgreSQL.
- `src/lakehouse/quality/rules.py` centraliza regras iniciais de qualidade.
- `src/lakehouse/silver/sales.py` prepara datasets Silver limpos e resultados de validação.
- `src/lakehouse/silver/service.py` persiste os datasets Silver e o pacote de qualidade em disco.
