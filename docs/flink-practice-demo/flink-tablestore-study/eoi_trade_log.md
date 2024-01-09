
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

SET 'execution.checkpointing.interval' = '5s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

```

## 基于DataGen模拟器的 Trade数仓增量一体操作




## ODS:  Kafak消费ods数据

通过json-file-sender或手动发送原生日志数据如下


```json
{
  "log": "2022-05-17T04:09:02.487660Z [Note] TransformInfo:20230201132710||201000042022039349092393||EXZCash||BR2580019||10000.00||27||0000||192.168.110.152||xyz",
  "prometheus": "cpu_second{instance\u003d\"hadoop01\", app_id\u003d\"app_202211260928\"} 2580.21",
  "logHeader": "1669166386000|320|1|ZGBX|node1002",
  "jsonArray": "[{\"metric\":\"cpu_second\",\"prometheus\":\"cpu_second{instance\u003d\\\"hadoop01\\\", app_id\u003d\\\"app_202211260928\\\"} 2580.21\",\"val\":2580.21},{\"metric\":\"heap_memory\",\"prometheus\":\"heap_memory{instance\u003d\\\"kafka6\\\", app_id\u003d\\\"app_20221123933\\\"} 10485760\",\"val\":10485760}]",
  "timestamp": 1679697889601
}
```

建立 ods_trade_log 表
```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TEMPORARY TABLE IF EXISTS ods_trade_log;
CREATE TEMPORARY TABLE ods_trade_log (
    log STRING,
    prometheus STRING,
    logHeader STRING,
    jsonArray STRING, 
    `timestamp` BIGINT, 
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3), 
    WATERMARK FOR event_time AS event_time
) WITH (
'connector' = 'kafka',
'topic' = 'testSourceTopic',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_trade_fts_gid',
'scan.startup.mode' = 'latest-offset',
'format' = 'json'
);

SELECT * FROM ods_trade_log;


```



## DIM 维表层



## DWD 明细层

```sql


SELECT 
  tdArr[1] as transTime,
  tdArr[2] as transId,
  tdArr[3] as transType,
  tdArr[4] as paymentCode,
  tdArr[5] as transAmount,
  tdArr[6] as usedTime,
  tdArr[7] as resultCode,
  tdArr[8] as ip
FROM (
  SELECT splitAsArray(REGEXP_EXTRACT(log, '^(.*)Z \[\w+\] TransformInfo:(.*)$', 2), '\|\|') as tdArr from ods_trade_log
) 


```


## DWS 聚合层 

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




