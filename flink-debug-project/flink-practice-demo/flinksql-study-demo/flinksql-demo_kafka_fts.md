
# Flink 基于Table-Store的实时数仓搭建Demo 

[toc]

## Demo 需求

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

## 基于DataGen模拟器的 Trade数仓增量一体操作

```sql

-- 1. dwd: 明细曾:  create a TEMPORARY ods_trade_detail,Table Store Catalog 仅支持默认的[table store]表, 即会用TableStore引擎存储文件系统; 
USE CATALOG test_mem_catalog;
SET 'execution.runtime-mode' = 'streaming';

DROP TABLE IF EXISTS dwd_trade_detail;
CREATE TEMPORARY TABLE dwd_trade_detail (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time AS localtimestamp,
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.trade_id.kind' = 'sequence',
'fields.trade_id.start' = '1',
'fields.trade_id.end' = '10000',
'fields.trade_type.kind' = 'random',
'fields.trade_type.min' = '201',
'fields.trade_type.max' = '206',
'fields.branch_id.kind' = 'random',
'fields.branch_id.min' = '3001',
'fields.branch_id.max' = '3008',
'fields.trade_amount.kind' = 'random',
'fields.trade_amount.min' = '4000',
'fields.trade_amount.max' = '4002',
'fields.result_code.min' = '-1',
'fields.result_code.max' = '1'
);

-- SELECT dwd_trade_detail.* , PROCTIME() AS op_time FROM dwd_trade_detail;
-- Ctl + C 退出 动态结果打印

-- 2. DIM 维表层: Branch Info, 分行信息 
DROP TABLE IF EXISTS dim_branch_info;
CREATE TABLE dim_branch_info (
branch_id INT PRIMARY KEY,
branch_name STRING,
branch_type INT,
province INT,
branch_manager STRING,
update_time TIMESTAMP(3),
WATERMARK FOR update_time AS update_time
) WITH (
'connector' = 'datagen', 
'fields.branch_id.kind' = 'sequence', 
'fields.branch_id.start' = '3001', 
'fields.branch_id.end' = '3009', 
'fields.branch_name.length' = '2', 
'fields.branch_type.kind' = 'random', 
'fields.branch_type.min' = '101', 
'fields.branch_type.max' = '103', 
'fields.province.kind' = 'random', 
'fields.province.min' = '601', 
'fields.province.max' = '609', 
'fields.branch_manager.length' = '1'
);

SELECT * FROM dim_branch_info;


--- 3. dws, 轻度聚合层, 求各维度每分钟的汇总指标

-- 3.1 Temporal Join , 动态拉链关联维表,丰富字段; 
DROP VIEW IF EXISTS dwd_trade_branch_info;
CREATE VIEW dwd_trade_branch_info AS 
SELECT
  dwd.* , dim.branch_name, dim.branch_type, dim.province, dim.branch_manager,
  timestampDiff(SECOND, dwd.trade_time, dim.update_time) AS timediff_sec
FROM dwd_trade_detail AS dwd
    LEFT JOIN dim_branch_info FOR SYSTEM_TIME AS OF dwd.trade_time AS dim
    ON dwd.branch_id = dim.branch_id
;
-- SELECT dwd.trade_id, dwd.branch_id, dim.branch_name, dim.province, timestampDiff(SECOND, dwd.trade_time, dim.update_time) AS timediff_sec,  DATE_FORMAT(dwd.trade_time, 'mm:ss.SSS') AS tradeT, DATE_FORMAT(dim.update_time, 'mm:ss.SSS') AS updateT 
-- FROM dwd_trade_detail AS dwd LEFT JOIN dim_branch_info FOR SYSTEM_TIME AS OF dwd.trade_time AS dim ON dwd.branch_id = dim.branch_id;

-- 3.2 多维度窗口聚合, 求每分钟各维度统计指标; 
DROP TABLE IF EXISTS dws_trade_summary_1m;
CREATE TABLE dws_trade_summary_1m (
win_time TIMESTAMP(3),
branch_type INT,
province INT,
branch_manager STRING,
branch_name STRING,
result_code INT,
cnt BIGINT,
trade_amount_sum DOUBLE,
query_time TIMESTAMP(3),
WATERMARK FOR win_time AS win_time
) WITH (
  'connector' = 'print'
) ;

INSERT INTO dws_trade_summary_1m 
SELECT window_start, branch_type, province, branch_manager, branch_name, result_code, 
       count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time
FROM TABLE(TUMBLE(TABLE dwd_trade_branch_info, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES)) 
GROUP BY window_start, branch_type, province, branch_manager, branch_name, result_code ;

-- print connector 只能作为Sink, 不能作为Source来查; 所以 需要到TM-log-stdout中查看; 


```


## Kafka RT(Real Time) Data Warehouse 
环境准备
```sql
DROP CATALOG IF EXISTS mem_catalog_kafka; 
CREATE CATALOG mem_catalog_kafka WITH (
 'type'='generic_in_memory'
);
USE CATALOG mem_catalog_kafka;

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'sql-client.execution.result-mode' = 'tableau';

```
准备Kafka元素数据: 利用DataGen往里面写
```sql

DROP TEMPORARY TABLE IF EXISTS kafkaprepare_datagen_source;
CREATE TEMPORARY TABLE kafkaprepare_datagen_source (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time AS localtimestamp,
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.trade_id.kind' = 'sequence',
'fields.trade_id.start' = '1',
'fields.trade_id.end' = '10000',
'fields.trade_type.kind' = 'random',
'fields.trade_type.min' = '201',
'fields.trade_type.max' = '206',
'fields.branch_id.kind' = 'random',
'fields.branch_id.min' = '3001',
'fields.branch_id.max' = '3008',
'fields.trade_amount.kind' = 'random',
'fields.trade_amount.min' = '4000',
'fields.trade_amount.max' = '4002',
'fields.result_code.min' = '-1',
'fields.result_code.max' = '1'
);
DROP TEMPORARY TABLE IF EXISTS kafkaprepare_kafka_sink;
CREATE TEMPORARY TABLE kafkaprepare_kafka_sink (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time TIMESTAMP(3)
) WITH (
'connector' = 'kafka',
'topic' = 'dwd_trade_detail',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json'
);

INSERT INTO kafkaprepare_kafka_sink SELECT * FROM kafkaprepare_datagen_source;

DROP TEMPORARY TABLE IF EXISTS kafkaprepare_kafka_sink;
DROP TEMPORARY TABLE IF EXISTS kafkaprepare_datagen_source;

```

```sql

-- 1. dwd: 明细曾:  create a TEMPORARY ods_trade_detail,Table Store Catalog 仅支持默认的[table store]表, 即会用TableStore引擎存储文件系统; 

DROP TABLE IF EXISTS dwd_trade_detail;
CREATE TABLE dwd_trade_detail (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time TIMESTAMP(3),
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'kafka',
'topic' = 'dwd_trade_detail',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json'
);

-- SELECT dwd_trade_detail.* , PROCTIME() AS op_time FROM dwd_trade_detail;
-- Ctl + C 退出 动态结果打印

-- 2. DIM 维表层: Branch Info, 分行信息 
DROP TABLE IF EXISTS dim_branch_info;
CREATE TABLE dim_branch_info (
branch_id INT PRIMARY KEY,
branch_name STRING,
branch_type INT,
province INT,
branch_manager STRING,
update_time TIMESTAMP(3),
WATERMARK FOR update_time AS update_time
) WITH (
'connector' = 'datagen', 
'fields.branch_id.kind' = 'sequence', 
'fields.branch_id.start' = '3001', 
'fields.branch_id.end' = '3009', 
'fields.branch_name.length' = '2', 
'fields.branch_type.kind' = 'random', 
'fields.branch_type.min' = '101', 
'fields.branch_type.max' = '103', 
'fields.province.kind' = 'random', 
'fields.province.min' = '601', 
'fields.province.max' = '609', 
'fields.branch_manager.length' = '1'
);

SELECT * FROM dim_branch_info;


--- 3. dws, 轻度聚合层, 求各维度每分钟的汇总指标

-- 3.1 Temporal Join , 动态拉链关联维表,丰富字段; 
DROP VIEW IF EXISTS dwd_trade_branch_info;
CREATE VIEW dwd_trade_branch_info AS 
SELECT
  dwd.* , dim.branch_name, dim.branch_type, dim.province, dim.branch_manager,
  timestampDiff(SECOND, dwd.trade_time, dim.update_time) AS timediff_sec
FROM dwd_trade_detail AS dwd
    LEFT JOIN dim_branch_info FOR SYSTEM_TIME AS OF dwd.trade_time AS dim
    ON dwd.branch_id = dim.branch_id
;
-- SELECT dwd.trade_id, dwd.branch_id, dim.branch_name, dim.province, timestampDiff(SECOND, dwd.trade_time, dim.update_time) AS timediff_sec,  DATE_FORMAT(dwd.trade_time, 'mm:ss.SSS') AS tradeT, DATE_FORMAT(dim.update_time, 'mm:ss.SSS') AS updateT 
-- FROM dwd_trade_detail AS dwd LEFT JOIN dim_branch_info FOR SYSTEM_TIME AS OF dwd.trade_time AS dim ON dwd.branch_id = dim.branch_id;

-- 3.2 多维度窗口聚合, 求每分钟各维度统计指标; 
DROP TABLE IF EXISTS dws_trade_summary_1m;
CREATE TABLE dws_trade_summary_1m (
win_time TIMESTAMP(3),
branch_type INT,
province INT,
branch_manager STRING,
branch_name STRING,
result_code INT,
cnt BIGINT,
trade_amount_sum DOUBLE,
query_time TIMESTAMP(3),
pk STRING, 
PRIMARY KEY (`pk`) NOT ENFORCED, 
WATERMARK FOR win_time AS win_time
) WITH (
'connector' = 'upsert-kafka',
'topic' = 'dws_trade_summary_1m',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_demo_gid',
'key.format' = 'json',
'key.json.ignore-parse-errors' = 'true',
'value.format' = 'json',
'value.json.fail-on-missing-field' = 'false',
'value.fields-include' = 'EXCEPT_KEY' 
);

SET 'execution.runtime-mode' = 'streaming';
INSERT INTO dws_trade_summary_1m 
SELECT window_start, branch_type, province, branch_manager, branch_name, result_code,  
       count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(branch_type AS STRING), CAST(province AS STRING), CAST(branch_manager AS STRING), CAST(branch_name AS STRING), CAST(result_code AS STRING)) pk 
FROM TABLE(TUMBLE(TABLE dwd_trade_branch_info, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES)) 
GROUP BY window_start, branch_type, province, branch_manager, branch_name, result_code ;

SELECT pk,branch_type, cnt, trade_amount_sum FROM dws_trade_summary_1m where branch_type = 101 ;


```













