SET 'execution.checkpointing.interval' = '10 s';
SET execution.result-mode=tableau;
SET execution.type=streaming;

DROP TABLE IF EXISTS ods_jrsc_log;
CREATE TABLE ods_jrsc_log (
  `logTypeName` STRING,
  `timestamp` STRING,
  `source` STRING,
  `offset` STRING,
  `measures` MAP<STRING, DOUBLE>,
  `dimensions` MAP<STRING, STRING>,
  `normalFields` MAP<STRING, STRING>,
  ts AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),
  WATERMARK FOR ts AS ts
) WITH (
      -- declare the external system to connect to
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'jrsc_log',
      'connector.startup-mode' = 'latest-offset',
      'connector.properties.bootstrap.servers' = '192.168.51.124:9092',
      'update-mode' = 'append',
      'format.type' = 'avro',
      'format.avro-schema' = '{
    "namespace": "com.zork.logs",
    "type": "record",
    "name": "logs",
    "fields": [
        {
            "name": "logTypeName",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "timestamp",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "source",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "offset",
            "type": [
                "string",
                "null"
            ]
        },
        {
            "name": "dimensions",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": "string"
                }
            ]
        },
        {
            "name": "measures",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": "double"
                }
            ]
        },
        {
            "name": "normalFields",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": "string"
                }
            ]
        }
    ]
}'
  )
;


DROP VIEW IF EXISTS JRSC_netty_log;
CREATE VIEW JRSC_netty_log AS
SELECT
    `logTypeName`,
    `ts` AS `timestamp`,
    `source`,
    `offset`,
    `measures`,
    `dimensions`,
    `normalFields`,
    IF(`dimensions`['appsystem'] IS NOT NULL, `dimensions`['appsystem'], '') AS appsystem,
    IF(`dimensions`['hostname'] IS NOT NULL, `dimensions`['hostname'], '') AS hostname,
    `measures`['time_cost'] AS time_cost,
    IF(`normalFields`['funcid'] IS NOT NULL, `normalFields`['funcid'], '') AS funcid,
    IF(`normalFields`['msg_level'] IS NOT NULL, `normalFields`['msg_level'], '') AS msg_level,
    IF(`normalFields`['msg_ip'] IS NOT NULL, `normalFields`['msg_ip'], '') AS msg_ip,
    IF(`normalFields`['msg_info'] IS NOT NULL, `normalFields`['msg_info'], '') AS msg_info
FROM ods_jrsc_log;


DROP TABLE IF EXISTS mall_interface_count_latency_1min;
CREATE TABLE mall_interface_count_latency_1min (
`metricsetname` STRING,
`timestamp` STRING,
`dimensions` MAP<STRING, STRING>,
`metrics` MAP<STRING, DOUBLE>
) WITH (
      'connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.topic' = 'dws_jrsc_mall_interface_count_latency_1min',
      'connector.startup-mode' = 'latest-offset',
      'connector.properties.bootstrap.servers' = '192.168.51.124:9092',
      'update-mode' = 'append',
      'format.type' = 'avro',
      'format.avro-schema' = '{
  "namespace": "com.zork.metrics",
  "type": "record",
  "name": "metrics",
  "fields": [
    {
      "name": "metricsetname",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "timestamp",
      "type": [
        "string",
        "null"
      ]
    },
    {
      "name": "dimensions",
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ]
    },
    {
      "name": "metrics",
      "type": [
        "null",
        {
          "type": "map",
          "values": "double"
        }
      ]
    }
  ]
}'
      )
;


INSERT INTO mall_interface_count_latency_1min
SELECT
    'mall_interface_count_latency_1min' AS `metricsetname`,
    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS+08:00') AS `timestamp`,
    Map['appsystem', appsystem, 'funcid', funcid, 'hostname', hostname] AS `dimensions`,
  Map['num', num, 'avgcost', avgcost] AS `metrics`
FROM
(
  SELECT
    appsystem,
    funcid,
    hostname,
    cast(count(*) as double) as num,
    cast(avg(time_cost) as double) as avgcost,
    TUMBLE_START(`timestamp`, INTERVAL '5' SECOND) as window_start
  FROM
    JRSC_netty_log
  WHERE
    msg_level like '%INFO%'
    and msg_ip is not null
  GROUP BY
    appsystem,
    funcid,
    hostname,
    TUMBLE(`timestamp`, INTERVAL '5' SECOND)
)
;








