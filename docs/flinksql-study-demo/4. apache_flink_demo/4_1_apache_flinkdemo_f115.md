# Apache Flink 官方案例代码


# Flink Table Api

```sql

CREATE TEMPORARY TABLE Orders (
    product AS 'product_1', 
    amount INT) 
WITH (
    'connector' = 'datagen',
    'rows-per-second' = '2',
    'fields.amount.kind' = 'sequence',
    'fields.amount.start' = '10',
    'fields.amount.end' = '10000'
);

select * from GlassOrders;


```

### StatementSet api

插入多条
```sql
CREATE TEMPORARY TABLE GlassOrders (
    product AS 'product_1', 
    amount INT) 
WITH ('connector' = 'print');

```


