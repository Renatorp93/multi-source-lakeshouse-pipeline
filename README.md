# Multi-Source Lakehouse Pipeline

Projeto de portfolio para demonstrar uma plataforma de Engenharia de Dados ponta a ponta com arquitetura Lakehouse, padrao Medallion e multiplas fontes de dados.

## Objetivo

Construir uma base evolutiva para ingestao, padronizacao, qualidade e consumo analitico de dados no dominio de vendas, usando API publica, PostgreSQL, arquivos CSV e streaming simulado.

## Stack Inicial

- Python 3.11+
- PySpark 3.5
- Delta Lake
- Java 17+ para execucoes Spark locais
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

## Dominio

Tema adotado: vendas.

Entidades iniciais:

- customers
- products
- orders
- order_items

## Fontes atuais

- API publica: DummyJSON (`users`, `products` e `carts`)
- PostgreSQL: tabelas normalizadas no schema `sales`
- CSV: exportacoes tabulares geradas a partir do mesmo snapshot da API

## Como comecar

1. Crie o arquivo `.env` a partir de `.env.example`.
2. Suba o PostgreSQL com `docker compose -f infra/docker-compose.yml up -d postgres`.
3. Crie um ambiente virtual e instale o projeto com `pip install -e .[dev]`.
4. Garanta um Java 17+ configurado em `JAVA_HOME` para executar cargas Spark.
5. Rode os testes com `pytest`.
6. Sincronize as fontes com `python scripts/sync_sales_sources.py`.
7. Carregue a Bronze com `python scripts/load_sales_bronze.py`.
8. Construa e persista a Silver com `python scripts/build_sales_silver.py`.
9. Construa e persista a Gold com `python scripts/build_sales_gold.py`.

## O que os scripts fazem

O comando `python scripts/sync_sales_sources.py`:

- busca clientes, produtos e carrinhos na API publica
- salva os payloads brutos em `storage/landing/api/`
- gera exportacoes CSV em `storage/landing/csv/`
- cria e popula tabelas no PostgreSQL no schema `sales`

O comando `python scripts/load_sales_bronze.py`:

- localiza o ultimo batch comum entre API e CSV na Landing
- le o mesmo conjunto de dados a partir de API, CSV e PostgreSQL
- padroniza schemas e adiciona `processing_timestamp` e `record_hash`
- grava tabelas Delta em `storage/bronze/{source}/{entity}/`

Na Silver, a base atual ja entrega:

- selecao da fonte preferencial para curadoria
- limpeza e padronizacao de clientes, produtos, pedidos e itens
- deduplicacao e filtros relacionais
- resultados de qualidade em memoria para regras estruturais, de conteudo e relacionais
- persistencia dos datasets curados e dos `quality_results` em `storage/silver/`

Na Gold, a base atual ja entrega:

- mart `daily_sales` com receita, ticket medio e volume por dia
- mart `customer_sales` com receita, quantidade de pedidos e ultima compra por cliente
- mart `product_sales` com unidades e receita por produto
- persistencia dos marts analiticos em `storage/gold/sales/`

## Principio de Desenvolvimento

O projeto segue uma abordagem orientada a TDD:

- novos comportamentos devem comecar por testes
- os testes devem validar a funcionalidade entregue, nao detalhes internos de implementacao
- servicos de orquestracao e funcoes puras sao preferidos para facilitar testes estaveis e legiveis

## Proximos Passos

- validar a execucao local da Bronze apos configurar Java 17+
- materializar Bronze e Silver em Delta quando o ambiente Spark local estiver completo
- evoluir a Gold para indicadores temporais adicionais e KPIs de conversao
- ligar a orquestracao com Airflow
