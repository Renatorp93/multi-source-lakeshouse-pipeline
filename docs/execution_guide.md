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

## Validação

```bash
pytest
```
