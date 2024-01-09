
准备 Catalog和Db
```sql

CREATE CATALOG test_mem_catalog WITH (
 'type'='generic_in_memory'
);
USE CATALOG test_mem_catalog;
show tables;

SET 'execution.checkpointing.interval' = '10 s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

```


# 数据类型基本转换

### Map展开

```sql
DROP TABLE IF EXISTS test_transaction_datagen;
create table test_transaction_datagen (
      trans_id BIGINT,
      trans_type as 'TType001',
      trans_org_id AS 3002,
      trans_amount AS 3.1415,
      result_code as '0000',
      trans_time as '2021-10-02 08:00:25',
      productImages as ARRAY['image1','image2'],
      trans_org_tags AS MAP['name', '机构1', 'orgType', '二级机构']
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1',
      'fields.trans_id.kind' = 'sequence',
      'fields.trans_id.start' = '1',
      'fields.trans_id.end' = '10'
      );

select * from test_transaction_datagen;

SELECT trans_id,trans_time, trans_amount, mapKey,mapValue
FROM test_transaction_datagen, UNNEST(trans_org_tags) as t(mapKey,mapValue);


```
UNNEST
将array类型的数据展开为多行单列形式，列名为column_name
将map类型的数据展开为多行多列形式，列名为key_name和value_name

```sql
--- ARRAY数组格式 打平
UNNEST(x) AS table_alias(column_name)
-- Map格式 打平
UNNEST(y) AS table(key_name,value_name)

```



### 嵌套json

```sql

DROP TABLE IF EXISTS jax_gtja_demo;
CREATE TABLE jax_gtja_demo (
    `measures` ROW(latence DOUBLE), 
    `normalFields` ROW(token_serial_no STRING, op_program STRING,op_branch_no STRING, op_site STRING, collecttime2 STRING,logchecktime STRING, message STRING, error_no STRING, op_way STRING,op_code STRING, SetLbmError STRING, branch_no STRING, user_code STRING, error_path STRING, msg_id STRING), 
    `offset` STRING,
    `logTypeName` STRING,
    `source` STRING,
    `timestamp` STRING,
    `dimensions` ROW(`latence` STRING, `threadid` STRING, `result` STRING, `date` STRING, `path` STRING, appprogramname STRING,`hostname` STRING,`func` STRING,`ip` STRING, `appsystem` STRING, `time` STRING)
) WITH (
'connector' = 'kafka',
'topic' = 'testSourceTopic2',
'properties.bootstrap.servers' = '192.168.110.79:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json'
);



```




