
# 准备 

###  Mem_catalog 准备
环境准备
```sql

SET 'execution.checkpointing.interval' = '10 s';
SET execution.result-mode=tableau;
SET execution.type=streaming;

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
'format' = 'avro'
);

INSERT INTO kafkaprepare_kafka_sink SELECT * FROM kafkaprepare_datagen_source;

DROP TEMPORARY TABLE IF EXISTS kafkaprepare_kafka_sink;
DROP TEMPORARY TABLE IF EXISTS kafkaprepare_datagen_source;


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
      'format' = 'avro'
      );

SELECT dwd_trade_detail.* , PROCTIME() AS op_time FROM dwd_trade_detail;


```









