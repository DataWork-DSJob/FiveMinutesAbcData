-- Default Mem Catalog

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
DROP TABLE IF EXISTS dwd_trade_detail_sink;
CREATE TABLE IF NOT EXISTS dwd_trade_detail_sink (
    t_id BIGINT,
    t_client STRING,
    t_time TIMESTAMP(3),
    t_amount DOUBLE,
    PRIMARY KEY (t_id) NOT ENFORCED
) WITH (
     'connector' = 'starrocks',
     'jdbc-url'='jdbc:mysql://192.168.51.103:19030',
     'load-url'='192.168.51.103:18030',
     'database-name' = 'test_db',
     'table-name' = 'dwd_trade_detail',
     'username' = 'bigdata',
     'password' = 'DB.2023.starrocks',
     'sink.buffer-flush.interval-ms' = '5000',
     'sink.buffer-flush.max-bytes' = '74002019',
     'sink.connect.timeout-ms' = '5000'
);
INSERT INTO dwd_trade_detail_sink SELECT * FROM ods_trade_detail;

CREATE TABLE IF NOT EXISTS dwd_trade_detail_source (
t_id BIGINT,
t_client STRING,
t_time TIMESTAMP(3),
t_amount DOUBLE,
PRIMARY KEY (t_id) NOT ENFORCED,
WATERMARK FOR t_time AS t_time - INTERVAL  '20' SECOND
) WITH (
'connector' = 'starrocks',
'jdbc-url'='jdbc:mysql://192.168.51.103:19030',
'scan-url'='192.168.51.103:18030',
'database-name' = 'test_db',
'table-name' = 'dwd_trade_detail',
'username' = 'bigdata',
'password' = 'DB.2023.starrocks'
);

SET 'execution.runtime-mode' = 'streaming';
select t_client, count(*) as cnt, sum(t_amount) as ta_sum from dwd_trade_detail_source group by t_client;
