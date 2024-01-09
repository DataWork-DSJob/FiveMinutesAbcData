-- CREATE CATALOG IF NOT EXISTS paimon_trade WITH ('type' = 'paimon', 'warehouse'='file:///tmp/paimon/idea_sqlfile');

CREATE CATALOG paimon_trade WITH ('type' = 'paimon', 'warehouse'='hdfs://bdnode103:9000/tmp/paimon_dw_trade');
USE CATALOG paimon_trade;

SET 'sql-client.execution.result-mode'='TABLEAU';
SET 'execution.checkpointing.interval' = '5 s';
SET 'execution.runtime-mode' = 'streaming';


-- ods by datagen

CREATE TEMPORARY TABLE IF NOT EXISTS ods_trade_detail (
    t_id BIGINT,
    t_client AS 'Client_A',
    t_time AS localtimestamp,
    t_amount DOUBLE
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '2',
      'fields.t_id.kind' = 'sequence',
      'fields.t_id.start' = '1',
      'fields.t_id.end' = '500000',
      'fields.t_amount.kind' = 'random',
      'fields.t_amount.min' = '91',
      'fields.t_amount.max' = '109'
);


-- Dwd: Detail
DROP TABLE IF EXISTS dws_trade_summary_10s;
CREATE TABLE IF NOT EXISTS dwd_trade_detail (
    t_id BIGINT,
    t_client STRING,
    t_time TIMESTAMP(3),
    t_amount DOUBLE,
    WATERMARK FOR t_time AS t_time - INTERVAL '1' MINUTE
) ;
INSERT INTO dwd_trade_detail SELECT * FROM ods_trade_detail;

-- dws: Summary
DROP TABLE IF EXISTS dws_trade_summary_10s ;
CREATE TABLE IF NOT EXISTS dws_trade_summary_10s (
    win_time TIMESTAMP(3),
    t_client     STRING,
    cnt          BIGINT,
    t_amount_sum DOUBLE,
    PRIMARY KEY (win_time, t_client) NOT ENFORCED,
    WATERMARK FOR win_time AS win_time - INTERVAL '1' MINUTE
) ;

INSERT INTO dws_trade_summary_10s
SELECT
    window_start AS win_time,
    t_client,
    count(*) cnt,
    SUM(t_amount) AS t_amount_sum
FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(t_time), INTERVAL '10' SECONDS))
GROUP BY window_start, t_client;


-- ADS  Application Data Service
DROP TABLE IF EXISTS ads_trade_summary_1d;
CREATE TABLE IF NOT EXISTS ads_trade_summary_1d (
    t_date       STRING,
    t_client     STRING,
    cnt          BIGINT,
    t_amount_sum    DOUBLE,
    t_amount_avg    DOUBLE,
    query_time   TIMESTAMP(3),
    PRIMARY KEY (t_date, t_client) NOT ENFORCED
) ;
INSERT INTO ads_trade_summary_1d
SELECT
    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS t_date,
    t_client,
    SUM(cnt) AS cnt,
    SUM(t_amount_sum) AS t_amount_sum,
    SUM(t_amount_sum) / SUM(cnt) AS t_amount_avg,
    PROCTIME() AS query_time
FROM TABLE(TUMBLE(TABLE dws_trade_summary_10s, DESCRIPTOR(win_time), INTERVAL '1' DAYS))
GROUP BY window_start, t_client;

