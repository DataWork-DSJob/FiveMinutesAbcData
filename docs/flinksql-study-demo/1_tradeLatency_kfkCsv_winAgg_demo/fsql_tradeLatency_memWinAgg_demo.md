
# 非TableStore, 窗口方式;

### 2.1 基于Memory的 catalog
```sql

DROP CATALOG IF EXISTS test_mem_catalog;
CREATE CATALOG test_mem_catalog WITH ('type'='generic_in_memory');
USE CATALOG test_mem_catalog;

SET 'execution.checkpointing.interval' = '10 s';
SET execution.result-mode=tableau;
SET execution.type=streaming;

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


测试用发送数据

```text
1,keyA, 1676101001, 10.0
2,keyA, 1676101022, 10.0
3,keyA, 1676101035, 10.0
4,keyA, 1676101005, 10.0

```





