
# 准备 

###  Mem_catalog 准备
环境准备
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



# 1. ODS: 原始采集层 




# 2. DIM: 维表层 

### 2.1 Datagen 模拟维表: dim_branch_info

```sql

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

```


# 3. DWD: 明细层,

## 3.1 构建 dwd_trade_detail 交易明细表; 
可从Kafka数据中建表, 或者datagen模拟表; 或者 table-store中从ods中读取生成表; 


### 3.1 来自Kafka数据的 dwd_trade_detail 交易明细 建表  

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
'scan.startup.mode' = 'latest-offset',
'format' = 'json'
);

-- SELECT dwd_trade_detail.* , PROCTIME() AS op_time FROM dwd_trade_detail;

```


从Kafka中消费 dwd_trade_datail数据,并直接写入 黑洞
```sql

SET pipeline.name='dwd_trade_detail to blackhole';
SET parallelism.default='4';

DROP TABLE IF EXISTS dwd_trade_detail_blackhole;
CREATE TABLE dwd_trade_detail_blackhole (
trade_id BIGINT,
trade_type INT,
branch_id INT,
trade_amount DOUBLE,
result_code INT,
trade_time TIMESTAMP(3),
WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'blackhole'
);

INSERT INTO dwd_trade_detail_blackhole SELECT  *  FROM dwd_trade_detail;

```

### 3.2 来自 DataGen 模拟器中 dwd_trade_detail 交易明细 建表

```sql
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

```


## 3.2 dwd_trade中相关数据的准备 


### 3.2.1  DataGen 模拟 dwd_trade_detail 并发送至Kafka topic; 

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


### 3.2.2 使用 data-sender直接发送 trade数据到  Kafka 

trade_json.json 中数据; 
```json
{
  "trade_id": 3,
  "trade_type": 203,
  "branch_id": 3003,
  "trade_amount": 4000.00,
  "result_code": 1,
  "trade_time": "2023-03-15 15:42:20.47"
}
```

利用data-sender模块的start.sh脚本, 发送 trade_json.json数据;
```shell

./bin/start.sh --jsonTemplateFile /home/bigdata/json-file-kafka-sender-0.1.3/config/trade_json.json --topic dwd_trade_detail --recordsPerSecond 20000

```



# 4. DWS: 轻度聚合层

### 4.1  简单 dws_trade_summary_1m 写入到upsert-kafka

```sql

SET 'execution.runtime-mode' = 'streaming';

DROP TABLE IF EXISTS dws_trade_summary_1m;
CREATE TABLE dws_trade_summary_1m (
win_time TIMESTAMP(3),
trade_type INT,
branch_id INT,
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

INSERT INTO dws_trade_summary_1m 
SELECT window_start,trade_type, branch_id, result_code, 
       count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(trade_type AS STRING), CAST(branch_id AS STRING), CAST(result_code AS STRING)) pk 
FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES)) 
GROUP BY window_start, trade_type, branch_id, result_code ;


```


### 4.2  大宽表dws: 多维表字段的 维度聚合层

```sql

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
DROP TABLE IF EXISTS dws_trade_branch_summary_1m;
CREATE TABLE dws_trade_branch_summary_1m (
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
'topic' = 'dws_trade_branch_summary_1m',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_demo_gid',
'key.format' = 'json',
'key.json.ignore-parse-errors' = 'true',
'value.format' = 'json',
'value.json.fail-on-missing-field' = 'false',
'value.fields-include' = 'EXCEPT_KEY' 
);

SET 'execution.runtime-mode' = 'streaming';
INSERT INTO dws_trade_branch_summary_1m 
SELECT window_start, branch_type, province, branch_manager, branch_name, result_code,  
       count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(branch_type AS STRING), CAST(province AS STRING), CAST(branch_manager AS STRING), CAST(branch_name AS STRING), CAST(result_code AS STRING)) pk 
FROM TABLE(TUMBLE(TABLE dwd_trade_branch_info, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES)) 
GROUP BY window_start, branch_type, province, branch_manager, branch_name, result_code ;

SELECT pk,branch_type, cnt, trade_amount_sum FROM dws_trade_branch_summary_1m where branch_type = 101 ;


```


# 5. ADS: 应用层













