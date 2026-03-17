# Arquitetura Inicial

## Visão Geral

O projeto segue uma arquitetura Lakehouse com padrão Medallion, separando responsabilidades por camada e priorizando reprocessamento, observabilidade e expansão futura para ambientes como Azure Databricks.

## Decisões já aplicadas

- `src/lakehouse` concentra toda a lógica da aplicação.
- `storage/` representa o data lake local por camadas.
- `configs/base.yml` centraliza defaults não sensíveis.
- `.env` sobrescreve parâmetros por ambiente.
- `infra/docker-compose.yml` sobe a infraestrutura local inicial.

## Componentes da primeira entrega

- PostgreSQL como fonte transacional simulada.
- Spark com Delta configurado no código da aplicação.
- logging padronizado para jobs.
- estrutura pronta para evoluir para Landing, Bronze, Silver e Gold.
