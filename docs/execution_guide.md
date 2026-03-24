# Guia de Execucao

## Preparacao

1. Copie `.env.example` para `.env`.
2. Ajuste portas e credenciais se necessario.

## Infraestrutura local

Suba o banco com:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

Para subir tambem o Airflow:

```bash
docker compose -f infra/docker-compose.yml up -d airflow-init airflow-webserver airflow-scheduler
```

Por padrao, o acesso fica com:

- usuario: `admin`
- senha: `admin`

## Ambiente Python

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## Java para Spark

Para executar cargas com Spark e Delta localmente, configure um Java 17+ no ambiente com `JAVA_HOME`.

## Validacao

```bash
pytest
```

## Sincronizacao das fontes

```bash
python scripts/sync_sales_sources.py
```

Esse comando:

- consome a API publica DummyJSON
- salva JSON bruto em `storage/landing/api/`
- gera CSVs em `storage/landing/csv/`
- cria e popula tabelas no PostgreSQL

## Carga Bronze

```bash
python scripts/load_sales_bronze.py
```

Esse comando:

- identifica o ultimo batch comum da Landing
- padroniza os dados de API, CSV e PostgreSQL
- grava Delta por fonte e entidade na camada Bronze

## Persistencia Silver

```bash
python scripts/build_sales_silver.py
```

Esse comando:

- monta os datasets Bronze disponiveis em memoria
- escolhe a melhor fonte disponivel para curadoria
- gera datasets Silver limpos
- persiste os datasets e os `quality_results` em `storage/silver/`

## Quality Gate

```bash
python scripts/check_sales_quality.py
```

Esse comando:

- localiza o `quality_results` mais recente da Silver
- resume as regras avaliadas no batch
- falha rapidamente quando existe qualquer regra com status diferente de `passed`

## Alerta Operacional

```bash
python scripts/alert_sales_quality.py
```

Esse comando:

- reaproveita o mesmo resumo do gate de qualidade
- emite uma mensagem operacional legivel para logs e observabilidade
- foi desenhado para rodar no Airflow quando o gate bloqueia a promocao

## Persistencia Gold

```bash
python scripts/build_sales_gold.py
```

Esse comando:

- reaproveita a Silver persistida do batch corrente
- agrega marts analiticos de vendas por dia, mes, cliente e produto
- adiciona KPIs como `buyer_conversion_rate`, `discount_rate` e `average_items_per_order`
- persiste os marts em `storage/gold/sales/`

## Orquestracao Airflow

A DAG `sales_medallion_pipeline` encadeia:

- sincronizacao das fontes
- carga Bronze
- persistencia Silver
- quality gate da Silver
- persistencia Gold
- alerta operacional em caso de falha do gate

O Webserver fica disponivel em `http://localhost:8080`.
