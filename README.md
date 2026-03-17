# Multi-Source Lakehouse Pipeline

Projeto de portfólio para demonstrar uma plataforma de Engenharia de Dados ponta a ponta com arquitetura Lakehouse, padrão Medallion e múltiplas fontes de dados.

## Objetivo

Construir uma base evolutiva para ingestão, padronização, qualidade e consumo analítico de dados no domínio de vendas, usando API pública, PostgreSQL, arquivos CSV e streaming simulado.

## Stack Inicial

- Python 3.11+
- PySpark 3.5
- Delta Lake
- Java 17+ para execuções Spark locais
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
4. Garanta um Java 17+ configurado em `JAVA_HOME` para executar cargas Spark.
5. Rode os testes com `pytest`.
6. Sincronize as fontes com `python scripts/sync_sales_sources.py`.
7. Carregue a Bronze com `python scripts/load_sales_bronze.py`.

## O que o script faz

O comando `python scripts/sync_sales_sources.py`:

- busca clientes, produtos e carrinhos na API pública
- salva os payloads brutos em `storage/landing/api/`
- gera exportações CSV em `storage/landing/csv/`
- cria e popula tabelas no PostgreSQL no schema `sales`

O comando `python scripts/load_sales_bronze.py`:

- localiza o último batch comum entre API e CSV na Landing
- lê o mesmo conjunto de dados a partir de API, CSV e PostgreSQL
- padroniza schemas e adiciona `processing_timestamp` e `record_hash`
- grava tabelas Delta em `storage/bronze/{source}/{entity}/`

## Próximos Passos

- validar a execução local da Bronze após configurar Java 17+
- iniciar a camada Silver com limpeza e validação
- ligar a orquestração com Airflow
