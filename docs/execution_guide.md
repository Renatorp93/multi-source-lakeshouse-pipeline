# Guia de Execução

## Preparação

1. Copie `.env.example` para `.env`.
2. Ajuste portas e credenciais se necessário.

## Infraestrutura local

Suba o banco com:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

## Ambiente Python

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## Java para Spark

Para executar cargas com Spark e Delta localmente, configure um Java 17+ no ambiente com `JAVA_HOME`.

## Validação

```bash
pytest
```

## Sincronização das fontes

```bash
python scripts/sync_sales_sources.py
```

Esse comando:

- consome a API pública DummyJSON
- salva JSON bruto em `storage/landing/api/`
- gera CSVs em `storage/landing/csv/`
- cria e popula tabelas no PostgreSQL

## Carga Bronze

```bash
python scripts/load_sales_bronze.py
```

Esse comando:

- identifica o último batch comum da Landing
- padroniza os dados de API, CSV e PostgreSQL
- grava Delta por fonte e entidade na camada Bronze

## Fundação Silver

A curadoria inicial da Silver e o motor mínimo de qualidade já estão implementados nos módulos:

- `src/lakehouse/quality/rules.py`
- `src/lakehouse/silver/sales.py`
- `src/lakehouse/silver/service.py`

Nesta etapa, o foco está no comportamento e nas regras de negócio validadas por testes.

## Persistência Silver

```bash
python scripts/build_sales_silver.py
```

Esse comando:

- monta os datasets Bronze disponíveis em memória
- escolhe a melhor fonte disponível para curadoria
- gera datasets Silver limpos
- persiste os datasets e os `quality_results` em `storage/silver/`
