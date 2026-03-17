# Decisão IA 003 - Estratégia de ambiente inicial

Data: 2026-03-17

## Decisão

Usar:

- PostgreSQL em Docker Compose
- Spark e Delta configurados na aplicação Python
- parâmetros centralizados em `configs/base.yml` e sobrescritos por `.env`

## Motivo

Esse desenho simplifica o início do projeto, mantém aderência ao guideline e evita acoplamento prematuro com uma infraestrutura mais pesada antes das primeiras pipelines.
