# Multi-Source Lakehouse Pipeline

Projeto de portfólio para demonstrar uma plataforma de Engenharia de Dados ponta a ponta com arquitetura Lakehouse, padrão Medallion e múltiplas fontes de dados.

## Objetivo

Construir uma base evolutiva para ingestão, padronização, qualidade e consumo analítico de dados vindos de APIs, PostgreSQL, arquivos CSV e streaming simulado.

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
|-- IA_DECISIONS/
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

## Fase Atual

Fase inicial implementada:

- estrutura do repositório
- configuração base do projeto
- ambiente local com Docker Compose
- módulos de configuração, logging e Spark com Delta
- testes iniciais da fundação

## Como começar

1. Crie o arquivo `.env` a partir de `.env.example`.
2. Suba o PostgreSQL com `docker compose -f infra/docker-compose.yml up -d postgres`.
3. Crie um ambiente virtual e instale o projeto com `pip install -e .[dev]`.
4. Rode os testes com `pytest`.

## Próximos Passos

- popular o PostgreSQL com dados fake
- gerar exportações CSV
- integrar uma API pública
- iniciar a camada Landing
