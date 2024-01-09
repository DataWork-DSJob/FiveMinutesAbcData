
# Flink 基于 Paimon 的实时数仓搭建Demo 


###  准备好, 按顺序发送如下数据
```text
1,keyA, 1676101001, 10.0
2,keyA, 1676101022, 10.0
3,keyA, 1676101035, 10.0
4,keyA, 1676101005, 10.0

```

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



# Linux Sql-Client Demo Sql

```sql


CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file:/tmp/paimon');
USE CATALOG paimon;

-- 要设置短的 才能尽快输出
SET 'execution.checkpointing.interval' = '2s'; 
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';


DROP TEMPORARY TABLE IF EXISTS ods_trade_csv;
CREATE TEMPORARY TABLE ods_trade_csv (
    record_id BIGINT,
    group_key STRING,
    time_sec BIGINT,
    amount DOUBLE, 
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec)), 
    WATERMARK FOR event_time AS event_time
) WITH (
'connector' = 'kafka',
'topic' = 'ods_trade_csv',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'testGroupId_winIdea',
'scan.startup.mode' = 'latest-offset',
'format' = 'csv'
)
;
-- select * from ods_trade_csv 


DROP TABLE IF EXISTS dws_trade_summary_10s
;
CREATE TABLE dws_trade_summary_10s (
    win_time TIMESTAMP(3),
    group_key STRING,
    cnt BIGINT,
    amount_sum DOUBLE,
    query_time TIMESTAMP(3),
    pk STRING,
    PRIMARY KEY (`pk`) NOT ENFORCED,
    WATERMARK FOR win_time AS win_time
)
;

INSERT INTO dws_trade_summary_10s
SELECT window_start, group_key,
       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk
FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))
GROUP BY window_start, group_key
;


-- 流查询 
SET 'execution.runtime-mode' = 'streaming' ;
SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s ;

-- 批查询, 查最终结果 
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s



```

### 将 Paimon中 dws层数据 写出到 ads层 kafka, clickhouse, startRD中

```sql


DROP TEMPORARY TABLE IF EXISTS dws_trade_summary_10s_kafka
;
CREATE TEMPORARY TABLE dws_trade_summary_10s_kafka (
    win_time TIMESTAMP(3),
    group_key STRING,
    cnt BIGINT,
    amount_sum DOUBLE,
    query_time TIMESTAMP(3),
    pk STRING,
    PRIMARY KEY (`pk`) NOT ENFORCED,
    WATERMARK FOR win_time AS win_time
)
WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'dws_trade_summary_10s_kafka',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flinksql_demo_gid',
    'key.format' = 'json',
    'key.json.ignore-parse-errors' = 'true',
    'value.format' = 'json',
    'value.json.fail-on-missing-field' = 'false',
    'value.fields-include' = 'EXCEPT_KEY'
 )
;

INSERT INTO dws_trade_summary_10s_kafka SELECT * FROM dws_trade_summary_10s
;




```


