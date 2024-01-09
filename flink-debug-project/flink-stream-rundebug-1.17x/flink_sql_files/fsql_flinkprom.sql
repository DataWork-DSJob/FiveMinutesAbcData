-- CREATE CATALOG IF NOT EXISTS paimon_trade WITH ('type' = 'paimon', 'warehouse'='file:///tmp/paimon/idea_sqlfile');

CREATE CATALOG paimon_flinkprom WITH ('type' = 'paimon', 'warehouse'='hdfs://bdnode103:9000/tmp/paimon_flinkprom');
USE CATALOG paimon_flinkprom;

SET 'sql-client.execution.result-mode'='TABLEAU';
SET 'execution.checkpointing.interval' = '5 s';
SET 'execution.runtime-mode' = 'streaming';

-- 设置状态存储, rocksdb, 否则内存可能爆掉;
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 'hdfs://bdnode103:9000/tmp/flink_idea/checkpoint';
SET 'state.savepoint.dir' = 'hdfs://bdnode103:9000/tmp/flink_idea/savepoint';
SET state.backend.rocksdb.localdir = 'file:///D:/tmp/flink_rocksdb/db2';


--- 设置状态过期
-- SET 'table.exec.state.ttl' = '1m';


-- Dwd: Detail
CREATE TABLE IF NOT EXISTS dwd_flinkprom_instant (
    `metricTime` TIMESTAMP(3),
    `metricName` VARCHAR,
    `metricValue` DOUBLE,
    `job_id` VARCHAR,
    `job_name` VARCHAR,
    `tm_id` VARCHAR,
    `host` VARCHAR,
    `task_id` VARCHAR,
    `taskType` INT,
    `task_attempt_num` INT,
    `operator_id` VARCHAR,
    `operatorType` INT,
    `app_resource` VARCHAR,
    `queryTime` BIGINT,
    `label_pk` VARCHAR(32),
    `metric_pk` VARCHAR(32),
    WATERMARK FOR `metricTime` AS `metricTime` - INTERVAL '10' SECOND
) WITH (
    'path' = 'hdfs://bdnode103:9000/tmp/paimon_flinkprom/default.db/dwd_flinkprom_instant'
);


-- dws: Summary on Paimon
DROP TABLE IF EXISTS dws_flinkprom_1min;
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
);

INSERT INTO dws_flinkprom_1min
SELECT
    window_start AS metricTime,
    metric_pk, metricName, label_pk, job_name, job_id, host, tm_id, task_id, taskType, operator_id, operatorType, app_resource,
    60 AS duration_sec,
    count(*) AS cnt,
    ROUND(last_value(metricValue) - first_value(metricValue),5) AS mval_delta,
    ROUND((last_value(metricValue) - first_value(metricValue)) / 60,5) AS mval_rate,
    ROUND(sum(metricValue),5) AS mval_sum,
    ROUND(avg(metricValue),5) AS mval_avg,
    ROUND(min(metricValue),5) AS mval_min,
    ROUND(max(metricValue),5) AS mval_max,
    ROUND(first_value(metricValue),5) AS mval_first,
    ROUND(last_value(metricValue),5) AS mval_last,
    PROCTIME() AS queryTime
FROM TABLE(TUMBLE(TABLE dwd_flinkprom_instant, DESCRIPTOR(metricTime), INTERVAL '1' MINUTES))
GROUP BY window_start, metric_pk, metricName, label_pk, job_name, job_id, host, tm_id, task_id,taskType, operator_id, operatorType, app_resource;

SET 'execution.runtime-mode' = 'batch';
SELECT * FROM dws_flinkprom_1min limit 10;

SET 'execution.runtime-mode' = 'streaming';



-- ads: flinkprom_ads_1day
DROP TABLE IF EXISTS ads_flinkprom_1day;
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
);

INSERT INTO ads_flinkprom_1day
SELECT
    DATE_FORMAT(metricTime, 'yyyy-MM-dd') AS metric_day,
    metric_pk, metricName, label_pk,
    86400 AS duration_sec,
    count(*) AS cnt,
    SUM(cnt) AS cnt_sum,
    ROUND(SUM(mval_delta), 3) AS delta_sum,
    ROUND(SUM(mval_delta) / SUM(cnt), 3) AS rate_bydelta,
    ROUND(SUM(mval_sum), 3) AS sum_sum,
    ROUND(SUM(mval_sum) / SUM(cnt), 3) AS avg_bysumcnt,
    MIN(mval_min) AS min_min,
    MAX(mval_max) AS max_max,
    MIN(mval_first) AS first_min,
    MAX(mval_last) AS last_max,
    PROCTIME() AS queryTime
FROM dws_flinkprom_1min
GROUP BY DATE_FORMAT(metricTime, 'yyyy-MM-dd'), metric_pk, metricName, label_pk;


SELECT queryTime, metric_day, metricName, label_pk, cnt, cnt_sum, delta_sum
FROM ads_flinkprom_1day where metricName = 'flink_taskmanager_job_task_operator_numRecordsOutPerSecond';
