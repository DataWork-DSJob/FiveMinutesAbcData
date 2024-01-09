
# Flink 基于Table-Store的实时数仓搭建Demo 

[toc]

## Demo 需求

准备 Catalog和Db
```sql

DROP CATALOG IF EXISTS table_store_catalog;
CREATE CATALOG table_store_catalog WITH (
  'type'='table-store',
  'warehouse'='file:/tmp/table_store'
);

USE CATALOG table_store_catalog;
show tables;

SET 'execution.checkpointing.interval' = '2s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';


```

## 基于DataGen模拟器的 Trade数仓增量一体操作



###  准备好, 按顺序发送如下数据

1,keyA, 1676101001, 10.0
2,keyA, 1676101005, 10.0
3,keyA, 1676101003, 10.1
4,keyA, 1676101022, 10.0
5,keyA, 1676101004, 10.2
6,keyA, 1676101023, 10.0
7,keyA, 1676101021, 10.1
8,keyA, 1676101032, 10.0
9,keyA, 1676101033, 10.0
10,keyA, 1676101034, 10.0
11,keyA, 1676101031, 10.1
12,keyA, 1676101035, 10.0
13,keyA, 1676101002, 10.3
14,keyA, 1676102042, 10.0
15,keyA, 1676102043, 10.0
16,keyA, 1676101024, 10.3
17,keyA, 1676102044, 10.0
18,keyA, 1676102041, 10.1
19,keyA, 1676102045, 10.0
20,keyA, 1676105051, 10.0
21,keyA, 1676105052, 10.0
22,keyA, 1676101025, 10.4
23,keyA, 1676105053, 10.0
24,keyA, 1676105054, 10.0
25,keyA, 1676105055, 10.0



### Kafak消费ods数据

```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TEMPORARY TABLE IF EXISTS ods_trade_csv;
CREATE TEMPORARY TABLE ods_trade_csv (
    record_id BIGINT,
    group_key STRING,
    time_sec BIGINT,
    amount DOUBLE, 
    event_time AS TO_TIMESTAMP_LTZ(time_sec * 1000, 3), 
    WATERMARK FOR event_time AS event_time
) WITH (
'connector' = 'kafka',
'topic' = 'ods_trade_csv',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'latest-offset',
'format' = 'csv'
);
-- select * from ods_trade_csv;

```

明细层 dwd

```sql


-- 2   窗口聚合, 求每分钟各维度统计指标; 
SET 'execution.runtime-mode' = 'streaming';
DROP TABLE IF EXISTS dws_trade_summary_10s;
CREATE TABLE dws_trade_summary_10s (
      win_time TIMESTAMP(3),
      group_key STRING,
      cnt BIGINT,
      amount_sum DOUBLE,
      query_time TIMESTAMP(3),
      pk STRING,
      PRIMARY KEY (`pk`) NOT ENFORCED,
      WATERMARK FOR win_time AS win_time
);

INSERT INTO dws_trade_summary_10s
SELECT window_start, group_key,
       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk
FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))
GROUP BY window_start, group_key;

SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s ;


-- 批查询, 查最终结果 
SET 'execution.runtime-mode' = 'batch';
SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s ;




```






# 非TableStore, 窗口方式;

### 2.1 基于Memory的 catalog
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


### 2.2 Kafak消费ods数据

```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TEMPORARY TABLE IF EXISTS ods_trade_csv;
CREATE TEMPORARY TABLE ods_trade_csv (
    record_id BIGINT,
    group_key STRING,
    time_sec BIGINT,
    amount DOUBLE, 
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec)), 
    WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE
) WITH (
'connector' = 'kafka',
'topic' = 'ods_trade_csv',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'latest-offset',
'format' = 'csv'
)
;

-- select * from ods_trade_csv;

```

明细层 dwd

```sql


-- 2   窗口聚合, 求每分钟各维度统计指标; 

SELECT
    group_key,  TUMBLE_START(event_time, INTERVAL '10' SECOND) as window_start,
    count(*) AS cnt, sum(amount) AS amount_sum,
    PROCTIME() AS query_time
FROM ods_trade_csv
GROUP BY
    group_key,
    TUMBLE(event_time, INTERVAL '10' SECOND)
;


SET 'execution.runtime-mode' = 'streaming';
DROP VIEW IF EXISTS dws_trade_summary_10s;
CREATE VIEW dws_trade_summary_10s AS 
SELECT window_start AS win_time, group_key,
       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk
FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))
GROUP BY window_start, group_key;


SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s ;


-- 批查询, 查最终结果 
SET 'execution.runtime-mode' = 'batch';
SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s ;




```







