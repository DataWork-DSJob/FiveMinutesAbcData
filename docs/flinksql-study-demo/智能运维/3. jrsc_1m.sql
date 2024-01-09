SET 'execution.checkpointing.interval' = '5 s';
SET execution.result-mode=tableau;
SET execution.type=streaming;

DROP TABLE IF EXISTS ods_system_log;
CREATE TABLE ods_system_log (
  `typeName` STRING,
  `timestamp` STRING,
  `metrics` MAP<STRING, DOUBLE>,
  `dimensions` MAP<STRING, STRING>,
  `otherFields` MAP<STRING, STRING>,
  ts AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),
  WATERMARK FOR ts AS ts
) WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'system_log',
      'connector.startup-mode' = 'latest-offset',
      'connector.properties.bootstrap.servers' = 'localhost:9092',
      'update-mode' = 'append',
      'format.type' = 'json'
)
;

DROP VIEW IF EXISTS dwd_system_log;
CREATE VIEW dwd_system_log AS
SELECT
    `typeName`,
    `ts` AS `timestamp`,
    `dimensions`,
    IF(`dimensions`['system_name'] IS NOT NULL, `dimensions`['system_name'], '') AS system_name,
    IF(`dimensions`['hostname'] IS NOT NULL, `dimensions`['hostname'], '') AS hostname,
    IF(`otherFields`['bus_code'] IS NOT NULL, `otherFields`['bus_code'], '') AS bus_code,
    IF(`otherFields`['log_level'] IS NOT NULL, `otherFields`['log_level'], '') AS log_level,
    `metrics`['time_cost'] AS time_cost
FROM ods_system_log ;







-- 第二层

DROP TABLE IF EXISTS dws_system_log_10s;
CREATE TABLE dws_system_log_10s (
`metric_name` STRING,
`win_time` STRING,
`dims` MAP<STRING, STRING>,
`metrics` MAP<STRING, DOUBLE>
) WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'dws_system_log_10s',
      'connector.startup-mode' = 'latest-offset',
      'connector.properties.bootstrap.servers' = 'localhost:9092',
      'update-mode' = 'append',
      'format.type' = 'json'
)
;

INSERT INTO dws_system_log_10s
SELECT
    'dws_system_log_10s' AS `metric_name`,
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS+08:00') AS `win_time`,
    Map['system_name', system_name, 'hostname', hostname, 'bus_code', bus_code] AS `dims`,
  Map['num', num, 'avgcost', avgcost] AS `metrics`
FROM
(
  SELECT
    system_name,
    hostname,
    bus_code,
    cast(count(*) as double) as num,
    cast(avg(time_cost) as double) as avgcost,
    TUMBLE_START(`timestamp`, INTERVAL '10' SECOND) as window_start
  FROM dwd_system_log 
  WHERE log_level like '%INFO%' 
  GROUP BY
    system_name,
    hostname,
    bus_code,
    TUMBLE(`timestamp`, INTERVAL '10' SECOND)
)
;


DROP VIEW IF EXISTS dws_system_log_10s_view;
CREATE VIEW dws_system_log_10s_view AS
SELECT
    system_name,
    hostname,
    bus_code,
    cast(count(*) as double) as num,
    cast(avg(time_cost) as double) as avgcost,
    TUMBLE_START(`timestamp`, INTERVAL '10' SECOND) as window_start
FROM dwd_system_log
WHERE log_level like '%INFO%'
GROUP BY
    system_name,
    hostname,
    bus_code,
    TUMBLE(`timestamp`, INTERVAL '10' SECOND)
;

--select * from dws_system_log_10s_view




