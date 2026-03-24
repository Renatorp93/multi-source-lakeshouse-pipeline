# Decisao 019 - Quality gate e alerta operacional antes da Gold

## Contexto

Os `quality_results` da Silver ja eram persistidos por batch, mas ainda nao influenciavam a promocao para Gold. Isso deixava a orquestracao incapaz de bloquear batches problematicos e de expor um resumo acionavel no Airflow.

## Decisao

Adicionar uma etapa explicita de quality gate entre Silver e Gold, acompanhada de uma task de alerta operacional.

- `check_sales_quality` avalia o arquivo mais recente de `quality_results` e falha quando existir qualquer regra reprovada
- `alert_quality_failure` roda com `trigger_rule="one_failed"` para publicar o resumo do bloqueio no log do orquestrador

## Justificativa

- transforma qualidade de dados em criterio real de promocao, e nao apenas metadado passivo
- reaproveita o contrato ja persistido pela Silver sem duplicar regras
- melhora a leitura operacional da DAG ao separar bloqueio e alerta

## Exemplo na implementacao

Arquivo de referencia: `src/lakehouse/orchestration/sales_pipeline.py`

```python
PipelineTaskDefinition(
    task_id="check_sales_quality",
    bash_command=f"cd {quoted_project_root} && python scripts/check_sales_quality.py",
    downstream_task_ids=("build_sales_gold", "alert_quality_failure"),
)
```

## Impacto esperado

Batches com falha de qualidade deixam de chegar na Gold durante a execucao orquestrada. Ao mesmo tempo, o Airflow passa a registrar um resumo objetivo das regras bloqueantes, facilitando diagnostico e demonstracao operacional do projeto.
