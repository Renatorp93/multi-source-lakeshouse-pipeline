# Decisão IA 005 - Inicialização do Spark

Data: 2026-03-17

## Decisão

Centralizar a criação da `SparkSession` em um módulo dedicado com suporte explícito a Delta Lake.

## Motivo

Isso reduz duplicação, melhora testabilidade e mantém a futura migração para execução orquestrada mais simples.
