-- CREATE CATALOG IF NOT EXISTS paimon_trade WITH ('type' = 'paimon', 'warehouse'='file:///tmp/paimon/idea_sqlfile');

CREATE CATALOG paimon_flinkprom WITH ('type' = 'paimon', 'warehouse'='hdfs://bdnode103:9000/tmp/paimon_flinkprom');
USE CATALOG paimon_flinkprom;

SET 'sql-client.execution.result-mode'='TABLEAU';
SET 'execution.checkpointing.interval' = '5 s';
SET 'execution.runtime-mode' = 'streaming';


-- dws: dws_flinkprom_1min, PaimonDws to StarRocks-Dws1min
CREATE TABLE IF NOT EXISTS dws_flinkprom_1min (
    metricTime TIMESTAMP(3),
    metric_pk VARCHAR(32),
    metricName VARCHAR(64),
    label_pk VARCHAR(32),
    job_name VARCHAR(256),
    job_id VARCHAR(128),
    host VARCHAR(128),
    tm_id VARCHAR(128),
    task_id VARCHAR(128),
    taskType INT,
    operator_id VARCHAR(128),
    operatorType INT,
    app_resource VARCHAR(128),
    duration_sec INT,
    cnt BIGINT,
    mval_delta DOUBLE,
    mval_rate DOUBLE,
    mval_sum DOUBLE,
    mval_avg DOUBLE,
    mval_min DOUBLE,
    mval_max DOUBLE,
    mval_first DOUBLE,
    mval_last DOUBLE,
    queryTime TIMESTAMP(3),
    PRIMARY KEY(metricTime, metricName, label_pk) NOT ENFORCED,
    WATERMARK FOR `metricTime` AS `metricTime` - INTERVAL '10' SECOND
) WITH (
      'path' = 'hdfs://bdnode103:9000/tmp/paimon_flinkprom/default.db/dws_flinkprom_1min'
);

DROP TEMPORARY TABLE IF EXISTS dws_flinkprom_1min_starrocks;
CREATE TEMPORARY TABLE IF NOT EXISTS dws_flinkprom_1min_starrocks (
    metricTime TIMESTAMP(3),
    metric_pk VARCHAR(32),
    metricName VARCHAR(64),
    label_pk VARCHAR(32),
    job_name VARCHAR(256),
    job_id VARCHAR(128),
    host VARCHAR(128),
    tm_id VARCHAR(128),
    task_id VARCHAR(128),
    taskType INT,
    operator_id VARCHAR(128),
    operatorType INT,
    app_resource VARCHAR(128),
    duration_sec INT,
    cnt BIGINT,
    mval_delta DOUBLE,
    mval_rate DOUBLE,
    mval_sum DOUBLE,
    mval_avg DOUBLE,
    mval_min DOUBLE,
    mval_max DOUBLE,
    mval_first DOUBLE,
    mval_last DOUBLE,
    queryTime TIMESTAMP(3),
    PRIMARY KEY(metricTime, metric_pk) NOT ENFORCED,
    WATERMARK FOR `metricTime` AS `metricTime` - INTERVAL '10' SECOND
) WITH (
      'connector' = 'starrocks',
      'jdbc-url'='jdbc:mysql://192.168.51.103:19030',
      'load-url'='192.168.51.103:18030',
      'database-name' = 'flinkprom_db',
      'table-name' = 'dws_flinkprom_1min',
      'username' = 'bigdata',
      'password' = 'DB.2023.starrocks',
      'sink.buffer-flush.interval-ms' = '5000',
      'sink.buffer-flush.max-bytes' = '74002019',
      'sink.connect.timeout-ms' = '5000'
);

INSERT INTO dws_flinkprom_1min_starrocks
SELECT * FROM dws_flinkprom_1min;


-- 2 ads: ads_flinkprom_1day, Paimon to StarRocks
CREATE TABLE IF NOT EXISTS ads_flinkprom_1day (
    metric_day VARCHAR(12),
    metric_pk VARCHAR(32),
    metricName VARCHAR(64),
    label_pk VARCHAR(32),
    duration_sec INT,
    cnt BIGINT,
    cnt_sum BIGINT,
    delta_sum DOUBLE,
    rate_bydelta DOUBLE,
    sum_sum DOUBLE,
    avg_bysumcnt DOUBLE,
    min_min DOUBLE,
    max_max DOUBLE,
    first_min DOUBLE,
    last_max DOUBLE,
    queryTime TIMESTAMP(3),
    PRIMARY KEY(metric_day, metric_pk) NOT ENFORCED
) WITH (
     'path' = 'hdfs://bdnode103:9000/tmp/paimon_flinkprom/default.db/ads_flinkprom_1day'
);

DROP TEMPORARY TABLE IF EXISTS ads_flinkprom_1day_starrocks;
CREATE TEMPORARY TABLE IF NOT EXISTS ads_flinkprom_1day_starrocks (
    metric_day VARCHAR(12),
    metric_pk VARCHAR(32),
    metricName VARCHAR(64),
    label_pk VARCHAR(32),
    duration_sec INT,
    cnt BIGINT,
    cnt_sum BIGINT,
    delta_sum DOUBLE,
    rate_bydelta DOUBLE,
    sum_sum DOUBLE,
    avg_bysumcnt DOUBLE,
    min_min DOUBLE,
    max_max DOUBLE,
    first_min DOUBLE,
    last_max DOUBLE,
    queryTime TIMESTAMP(3),
    PRIMARY KEY(metric_day, metric_pk) NOT ENFORCED
) WITH (
      'connector' = 'starrocks',
      'jdbc-url'='jdbc:mysql://192.168.51.103:19030',
      'load-url'='192.168.51.103:18030',
      'database-name' = 'flinkprom_db',
      'table-name' = 'ads_flinkprom_1day',
      'username' = 'bigdata',
      'password' = 'DB.2023.starrocks',
      'sink.buffer-flush.interval-ms' = '5000',
      'sink.buffer-flush.max-bytes' = '74002019',
      'sink.connect.timeout-ms' = '5000'
);

INSERT INTO ads_flinkprom_1day_starrocks
SELECT * FROM ads_flinkprom_1day;

