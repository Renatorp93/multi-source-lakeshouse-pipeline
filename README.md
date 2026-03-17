# Multi-Source Lakehouse Pipeline

Projeto de portfólio para demonstrar uma plataforma de Engenharia de Dados ponta a ponta com arquitetura Lakehouse, padrão Medallion e múltiplas fontes de dados.

## Objetivo

Construir uma base evolutiva para ingestão, padronização, qualidade e consumo analítico de dados no domínio de vendas, usando API pública, PostgreSQL, arquivos CSV e streaming simulado.

## Stack Inicial

- Python 3.11+
- PySpark 3.5
- Delta Lake
- PostgreSQL
- Docker Compose
- Pytest

## Estrutura do Projeto

```text
.
|-- configs/
|-- dags/
|-- docs/
|-- infra/
|-- logs/
|-- notebooks/
|-- scripts/
|-- src/lakehouse/
|-- storage/
`-- tests/
```

## Domínio

Tema adotado: vendas.

Entidades iniciais:

- customers
- products
- orders
- order_items

## Fontes atuais

- API pública: DummyJSON (`users`, `products` e `carts`)
- PostgreSQL: tabelas normalizadas no schema `sales`
- CSV: exportações tabulares geradas a partir do mesmo snapshot da API

## Como começar

1. Crie o arquivo `.env` a partir de `.env.example`.
2. Suba o PostgreSQL com `docker compose -f infra/docker-compose.yml up -d postgres`.
3. Crie um ambiente virtual e instale o projeto com `pip install -e .[dev]`.
4. Rode os testes com `pytest`.
5. Sincronize as fontes com `python scripts/sync_sales_sources.py`.

## O que o script faz

O comando `python scripts/sync_sales_sources.py`:

- busca clientes, produtos e carrinhos na API pública
- salva os payloads brutos em `storage/landing/api/`
- gera exportações CSV em `storage/landing/csv/`
- cria e popula tabelas no PostgreSQL no schema `sales`

## Próximos Passos

- converter Landing para Bronze em Delta Lake
- adicionar metadados completos por camada
- ligar a orquestração com Airflow
