# Decisao 020 - Gold consome a Silver persistida como contrato de entrada

## Contexto

Embora a documentacao ja tratasse a Gold como consumidora da Silver persistida, a implementacao ainda reconstruia a Silver dentro do servico Gold. Isso duplicava trabalho, enfraquecia o isolamento entre camadas e podia mascarar problemas de materializacao.

## Decisao

Fazer a Gold resolver o batch Silver persistido em disco antes de agregar os marts.

- o batch padrao passa a ser o mesmo batch mais recente identificado em `storage/silver/monitoring/quality_results`
- a carga Gold falha cedo quando nao encontra uma Silver completa para esse batch

## Justificativa

- alinha implementacao, documentacao e testes
- reforca o contrato entre camadas por artefato persistido
- evita reconstruir a Silver desnecessariamente durante a Gold

## Exemplo na implementacao

Arquivo de referencia: `src/lakehouse/gold/service.py`

```python
silver_result = silver_result_loader(settings, batch_id=batch_id)
silver_datasets = silver_loader(silver_result.dataset_paths)
gold_result = gold_builder(silver_datasets)
```

## Impacto esperado

A camada Gold passa a refletir de forma mais fiel um fluxo lakehouse real, consumindo apenas a Silver ja materializada e validada. Isso torna reprocessamento, depuracao e operacao da pipeline mais previsiveis.
