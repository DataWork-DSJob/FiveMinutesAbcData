
准备 Catalog和Db
```sql

CREATE CATALOG test_mem_catalog WITH (
 'type'='generic_in_memory'
);
USE CATALOG test_mem_catalog;
show tables;

SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.runtime-mode' = 'streaming';

SET execution.result-mode=tableau;
SET execution.type=batch;

```

flink-1.12 相关SET参数

```sql

SET 'execution.checkpointing.interval' = '10 s';

SET execution.result-mode=tableau;

SET execution.type=batch;
SET execution.type=streaming;


```


# 数据类型基本转换

### Map展开

```sql

SET execution.type=streaming;
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

```json
{
 "funcName": "test",
 "data": {
  "snapshots": [{
   "content_type": "application/x-gzip-compressed-jpeg",
   "url": "https://blog.csdn.net/xianpanjia4616"
  }],
  "audio": [{
   "content_type": "audio/wav",
   "url": " https://bss.csdn.net/m/topic/blog_star2020/detail?username=xianpanjia4616"
  }]
 },
 "resultMap": {
  "result": {
   "cover": "/data/test/log.txt"
  },
  "isSuccess": true
 },
 "meta": {
  "video_type": "normal"
 },
 "type": 2,
 "timestamp": 1610549997263,
 "arr": [{
  "address": "北京市海淀区",
  "city": "beijing"
 }, {
  "address": "北京市海淀区",
  "city": "beijing"
 }, {
  "address": "北京市海淀区",
  "city": "beijing"
 }],
 "map": {
  "flink": 456
 },
 "doublemap": {
  "inner_map": {
   "key": 123
  }
 }
}

```

```sql

CREATE TABLE kafka_source (
      funcName STRING,
      data ROW<snapshots ARRAY<ROW<content_type STRING,url STRING>>,audio ARRAY<ROW<content_type STRING,url STRING>>>,
      resultMap ROW<`result` MAP<STRING,STRING>,isSuccess BOOLEAN>,
      meta  MAP<STRING,STRING>,
      `type` INT,
      `timestamp` BIGINT,
      arr ARRAY<ROW<address STRING,city STRING>>,
      map MAP<STRING,INT>,
      doublemap MAP<STRING,MAP<STRING,INT>>,
      proctime as PROCTIME()
) WITH (
      'connector' = 'kafka', -- 使用 kafka connector
      'topic' = 'test',  -- kafka topic
      'properties.bootstrap.servers' = 'master:9092,storm1:9092,storm2:9092',  -- broker连接信息
      'properties.group.id' = 'jason_flink_test', -- 消费kafka的group_id
      'scan.startup.mode' = 'latest-offset',  -- 读取数据的位置
      'format' = 'json',  -- 数据源格式为 json
      'json.fail-on-missing-field' = 'true', -- 字段丢失任务不失败
      'json.ignore-parse-errors' = 'false'  -- 解析失败跳过
)
;

select
    funcName,
    doublemap['inner_map']['key'],
    count(data.snapshots[1].url),
    `type`,
    TUMBLE_START(proctime, INTERVAL '30' second) as t_start
from kafka_source
group by TUMBLE(proctime, INTERVAL '30' second),funcName,`type`,doublemap['inner_map']['key']
;

```


```sql

DROP TABLE IF EXISTS jax_gtja_demo;
CREATE TABLE jax_gtja_demo (
    `measures` ROW(latence DOUBLE),
    normalFields ROW(token_serial_no STRING, op_program STRING,op_branch_no STRING, op_site STRING, collecttime2 STRING,logchecktime STRING, message STRING, error_no STRING, op_way STRING,op_code STRING, SetLbmError STRING, branch_no STRING, user_code STRING, error_path STRING, msg_id STRING),
    `offset` STRING,
    logTypeName STRING,
    `source` STRING,
    `timestamp` STRING,
    dimensions ROW(latence STRING, threadid STRING, `result` STRING, `date` STRING, `path` STRING, appprogramname STRING,hostname STRING,`func` STRING,ip STRING, appsystem STRING, `time` STRING),
    ts AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),
    WATERMARK FOR ts AS ts
) WITH (
      'connector' = 'kafka',
      'topic' = 'testSourceTopic2',
      'properties.bootstrap.servers' = '192.168.110.79:9092',
      'properties.group.id' = 'flinksql_demo_gid',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json'
);

SELECT
    dimensions.appsystem,
    dimensions.hostname,
    dimensions.func,
    normalFields.error_no,
    cast(count(*) as double) as num,
    cast(avg(`measures`.latence) as double) as latency,
    cast(sum(case when `result` = '处理失败' then 1 else 0 end) as double) as failnum,
    TUMBLE_START (ts, INTERVAL '60' SECOND) AS `timestamp`
FROM
    jax_gtja_demo
GROUP BY
    dimensions.appsystem,
    dimensions.hostname,
    dimensions.func,
    normalFields.error_no,
    TUMBLE (ts, INTERVAL '60' SECOND)
;





```

