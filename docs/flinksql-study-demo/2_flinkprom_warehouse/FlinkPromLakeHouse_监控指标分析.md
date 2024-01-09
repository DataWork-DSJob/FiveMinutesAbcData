
# Flink Prom 普米指标分析


### 来自flink-metrics Topic的数据

```json

{
  "metricName": "flink_taskmanager_job_task_operator_numRecordsOut",
  "metricValue": 3.14,
  "metricTime": 1702210108193,
  "lineNo": 230,
  "host": "bdnode103_hjq_com",
  "app_resource": "application_1702182193084_0011"
  "job_name": "insert_into_paimon_default_dws_trade_gby_branch",
  "job_id": "78ae0d63776baf541340fb100cd12944",
  "tm_id": "container_1702182193084_0011_01_000002",
  "task_name": "GroupAggregate_5_____Calc_6_____ConstraintEnforcer_7_"
}

```



### DataGen 样例 

```json
{
  "metricTime": 1692210101003,
  "metricName": "flink_taskmanager_operator_numRecordsOut",
  "metricValue": 3.14,
  "host": "bdnode103",
  "job_name": "insert_into_tb_paimon"
}

```

```sql

DROP TEMPORARY TABLE IF EXISTS dwd_flinkprom_parsed;
CREATE TEMPORARY TABLE dwd_flinkprom_parsed (
    metricTime BIGINT,
    metricName as 'flink_taskmanager_job_task_operator_numRecordsOut',
    metricValue DOUBLE,
    host AS 'bdnode103',
    job_name AS 'insert_into_tb_paimon',
    event_time AS TO_TIMESTAMP_LTZ(metricTime, 3), 
    WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE 
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.metricTime.kind' = 'sequence',
'fields.metricTime.start' = '1692210101003',
'fields.metricTime.end' = '1692210201003',
'fields.metricValue.kind' = 'random',
'fields.metricValue.min' = '0',
'fields.metricValue.max' = '100'
);
SELECT * FROM dwd_flinkprom_parsed;


```

查询 Tumble 1 min查询 

```sql

SELECT
    host, 
    job_name, 
    window_start, 
    count(*) as cnt, 
    sum(metricValue) as mval_sum,
    min(metricValue) as mval_min,
    max(metricValue) as mval_max,
    avg(metricValue) as mval_avg,
    first_value(metricValue) as mval_first, 
    last_value(metricValue) as mval_last, 
    PROCTIME() AS query_time 
FROM TABLE(TUMBLE(TABLE dwd_flinkprom_parsed, DESCRIPTOR(event_time), INTERVAL '1' MINUTES))
GROUP BY host, job_name, window_start 
;

```










