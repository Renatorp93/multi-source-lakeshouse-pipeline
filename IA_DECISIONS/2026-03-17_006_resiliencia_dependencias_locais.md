# Decisão IA 006 - Resiliência para dependências locais

Data: 2026-03-17

## Decisão

Evitar import antecipado de integrações pesadas, como Delta Lake, durante a importação de módulos.

## Motivo

Isso permite que a base do projeto seja testada mesmo antes da instalação completa das dependências, o que acelera o bootstrap do repositório e reduz atrito na primeira execução.
