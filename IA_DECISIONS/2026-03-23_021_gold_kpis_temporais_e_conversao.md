# Decisao 021 - Gold com KPIs temporais e de conversao

## Contexto

A Gold ja entregava marts basicos por dia, cliente e produto, mas o README apontava como proximo passo a evolucao para indicadores temporais adicionais e KPIs de conversao. O projeto ainda nao possuia esse enriquecimento analitico.

## Decisao

Expandir a Gold sem introduzir novas fontes, reutilizando apenas os dados curados da Silver.

- criar o mart `monthly_sales`
- enriquecer os marts existentes com `discount_amount`, `discount_rate` e `average_items_per_order`
- expor `buyer_conversion_rate` como proporcao entre compradores e base total de clientes curados

## Justificativa

- amplia o valor analitico da Gold sem mudar o contrato das camadas anteriores
- entrega metricas mais proximas de um dashboard executivo real
- mantem o calculo explicavel e reproduzivel apenas com os dados ja disponiveis no portfolio

## Exemplo na implementacao

Arquivo de referencia: `src/lakehouse/gold/sales.py`

```python
monthly_sales = _build_monthly_sales(orders, items_by_order, total_customers=total_customers)
```

## Impacto esperado

A camada Gold passa a servir tanto para visoes operacionais de curto prazo quanto para leitura mensal de performance, com indicadores de desconto, eficiencia e conversao prontos para consumo em dashboards e demonstracoes.
