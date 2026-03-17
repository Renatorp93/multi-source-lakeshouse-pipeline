# Decisão IA 004 - Modelagem de configuração

Data: 2026-03-17

## Decisão

Implementar configuração em duas camadas:

- arquivo `configs/base.yml` para defaults versionados
- variáveis de ambiente para sobrescritas locais

## Motivo

Essa abordagem mantém o projeto reproduzível, evita hardcode de caminhos sensíveis e prepara o terreno para múltiplos ambientes sem complexidade excessiva neste início.
