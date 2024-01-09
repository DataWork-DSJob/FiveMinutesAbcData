

#  准备Catalog,配置和 加载Jar

准备 Catalog和Db
```sql

DROP CATALOG IF EXISTS table_store_catalog;
CREATE CATALOG table_store_catalog WITH (
  'type'='table-store',
  'warehouse'='file:/tmp/table_store'
);

USE CATALOG table_store_catalog;
SET 'execution.checkpointing.interval' = '10s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';
show tables;

-- 设置状态存储, rocksdb, 否则内存可能爆掉; 
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 'file:///tmp/flink/flink-checkpoints';
SET 'state.savepoint.dir' = 'file:///tmp/flink/flink-savepoints';

--- 设置状态过期 
SET 'table.exec.state.ttl' = '1m';


```

### Add jar和注册函数

```sql
ADD JAR '/opt/flink/udfs/udf-string-funcs-1.0-SNAPSHOT.jar';
SHOW JARS;

-- 添加函数
CREATE TEMPORARY SYSTEM FUNCTION splitAsArray AS 'com.github.flink.udfs.SplitAsArrayFunction';

SELECT splitAsArray('Allen,32,Shanghai,Baidu.com', ',') as arr, splitAsArray('Allen,32,Shanghai,Baidu.com', ',')[1] as name;


```


# 1. ODS:  Kafak消费ods数据

通过json-file-sender或手动发送原生日志数据如下

```json
{
  "log": "2022-05-17T04:09:02.487660Z [Note] TransformInfo:20230201132710||201000042022039349092393||EXZCash||3002||10000.00||27||0000||192.168.110.152||xyz",
  "prometheus": "cpu_second{instance\u003d\"hadoop01\", app_id\u003d\"app_202211260928\"} 2580.21",
  "logHeader": "1669166386000|320|1|ZGBX|node1002",
  "jsonArray": "[{\"metric\":\"cpu_second\",\"prometheus\":\"cpu_second{instance\u003d\\\"hadoop01\\\", app_id\u003d\\\"app_202211260928\\\"} 2580.21\",\"val\":2580.21},{\"metric\":\"heap_memory\",\"prometheus\":\"heap_memory{instance\u003d\\\"kafka6\\\", app_id\u003d\\\"app_20221123933\\\"} 10485760\",\"val\":10485760}]",
  "timestamp": 1679697889601
}
```

建立 ods_transaction_log 表
```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TEMPORARY TABLE IF EXISTS ods_transaction_log;
CREATE TEMPORARY TABLE ods_transaction_log (
    log STRING,
    prometheus STRING,
    logHeader STRING,
    jsonArray STRING, 
    `timestamp` BIGINT, 
    event_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3), 
    proc_as_eventtime AS localtimestamp, 
    WATERMARK FOR proc_as_eventtime AS proc_as_eventtime
) WITH (
'connector' = 'kafka',
'topic' = 'ods_transaction_log',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'flinksql_transaction_fts_gid',
'scan.startup.mode' = 'latest-offset',
'format' = 'json'
);

-- select * from ods_transaction_log;


```



# 2. DIM 维表层

```sql

-- 2. DIM 维表层: Branch Info, 分行信息 
SET 'execution.runtime-mode' = 'streaming';
DROP TEMPORARY TABLE IF EXISTS dim_transation_org_info;
CREATE TEMPORARY TABLE dim_transation_org_info (
trans_org_id INT,
trans_org_name STRING,
trans_org_type INT,
province INT,
trans_org_manager STRING,
update_time AS localtimestamp, 
PRIMARY KEY (`trans_org_id`) NOT ENFORCED 
) WITH (
'connector' = 'datagen', 
'fields.trans_org_id.kind' = 'sequence', 
'fields.trans_org_id.start' = '3001', 
'fields.trans_org_id.end' = '3009', 
'fields.trans_org_name.length' = '2', 
'fields.trans_org_type.kind' = 'random', 
'fields.trans_org_type.min' = '101', 
'fields.trans_org_type.max' = '103', 
'fields.province.kind' = 'random', 
'fields.province.min' = '601', 
'fields.province.max' = '609', 
'fields.trans_org_manager.length' = '1'
);

SELECT * FROM dim_transation_org_info;

```

# 3. DWD 明细层

* TEMPORARY SYSTEM: 没有数据库命名空间的临时系统 catalog function ，并覆盖系统内置的函数。

### 3.1 基于udf做日志解析和etl 

etl步骤:
- 步骤1, 切分提取字段: 20230201132710||202302011327109092393||EXZCash||BR2580019||10000.00||27||0000||192.168.110.152||xyz
- 步骤2, 解析时间和转换字段值类型

```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TABLE IF EXISTS dwd_transaction_detail;
CREATE TABLE dwd_transaction_detail (
    trans_id STRING,
    trans_type STRING,
    trans_org_id INT,
    trans_amount DOUBLE,
    used_time BIGINT,
    result_code STRING,
    trans_time TIMESTAMP(3),
    proc_as_eventtime TIMESTAMP(3),
    WATERMARK FOR proc_as_eventtime AS proc_as_eventtime 
);

INSERT INTO dwd_transaction_detail
SELECT
    tdArr[2] as trans_id,
    tdArr[3] as trans_type,
    CAST(tdArr[4] AS INT) as trans_org_id,
    CAST(tdArr[5] AS DOUBLE) as trans_amount,
    CAST(tdArr[6] AS BIGINT) as used_time,
    tdArr[7] as result_code, 
    TO_TIMESTAMP(tdArr[1],'yyyyMMddHHmmss') as trans_time,
    CAST(PROCTIME() AS TIMESTAMP(3)) as proc_as_eventtime 
FROM (
    SELECT splitAsArray(REGEXP_EXTRACT(log, '^(.*)Z \[\w+\] TransformInfo:(.*)$', 2), '\|\|') as tdArr from ods_transaction_log
);

SET 'execution.runtime-mode' = 'batch';
select trans_id,trans_type,trans_org_id, trans_amount, proc_as_eventtime from dwd_transaction_detail;
SET 'execution.runtime-mode' = 'streaming';


```


### 3.2 事实大宽表: etl明细 + join维表数据

```sql

-- 事实大宽表, 
DROP TABLE IF EXISTS dwd_transaction_org_detail;
CREATE TABLE dwd_transaction_org_detail (
trans_id STRING,
trans_type STRING,
trans_org_id INT,
trans_amount DOUBLE,
used_time BIGINT,
result_code STRING,
trans_time TIMESTAMP(3),
proc_as_eventtime TIMESTAMP(3),
trans_org_name STRING,
trans_org_type INT,
province INT,
trans_org_manager STRING,
timediff_sec INT,
-- PRIMARY KEY (`trans_id`) NOT ENFORCED,
WATERMARK FOR proc_as_eventtime AS proc_as_eventtime
);

-- 问题点: 设置state.ttl会过期后无法join到数据; 不设置会状态爆炸; 
-- SET 'table.exec.state.ttl' = '1m';

INSERT INTO dwd_transaction_org_detail 
SELECT
    dwd.* , dim.trans_org_name, dim.trans_org_type, dim.province, dim.trans_org_manager,
    timestampDiff(SECOND, dwd.trans_time, dim.update_time) AS timediff_sec
FROM (
         SELECT
             tdArr[2] as trans_id,
             tdArr[3] as trans_type,
             CAST(tdArr[4] AS INT) as trans_org_id,
             CAST(tdArr[5] AS DOUBLE) as trans_amount,
             CAST(tdArr[6] AS BIGINT) as used_time,
             tdArr[7] as result_code,
             TO_TIMESTAMP(tdArr[1],'yyyyMMddHHmmss') as trans_time,
             CAST(PROCTIME() AS TIMESTAMP(3)) as proc_as_eventtime
         FROM (SELECT splitAsArray(REGEXP_EXTRACT(log, '^(.*)Z \[\w+\] TransformInfo:(.*)$', 2), '\|\|') as tdArr from ods_transaction_log)
     ) AS dwd
         LEFT JOIN dim_transation_org_info AS dim
         ON dwd.trans_org_id = dim.trans_org_id
;

SET 'execution.runtime-mode' = 'batch';
select trans_id,trans_type,trans_org_id, trans_amount, proc_as_eventtime from dwd_transaction_org_detail;
SET 'execution.runtime-mode' = 'streaming';


```


# DWS 聚合层 

### 4.1 轻度聚合层 


```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TABLE IF EXISTS dws_transaction_org_summary_10s;
CREATE TABLE dws_transaction_org_summary_10s (
     win_time TIMESTAMP(3),
     trans_type STRING,
     trans_org_id INT,
     trans_org_name STRING,
     trans_org_type INT,
     province INT,
     trans_org_manager STRING,
     result_code STRING,
     cnt BIGINT,
     trans_amount_sum DOUBLE,
     trans_amount_avg DOUBLE,
     trans_amount_min DOUBLE,
     trans_amount_max DOUBLE,
     used_time_sum BIGINT,
     used_time_avg BIGINT,
     used_time_min BIGINT,
     used_time_max BIGINT,
     query_time TIMESTAMP(3),
     pk STRING,
     PRIMARY KEY (pk) NOT ENFORCED,
     WATERMARK FOR win_time AS win_time
);

INSERT INTO dws_transaction_org_summary_10s
SELECT window_start, trans_type, trans_org_id, trans_org_name, trans_org_type, province, trans_org_manager, result_code,
       count(*) cnt,
       sum(trans_amount) trans_amount_sum, avg(trans_amount) trans_amount_avg, min(trans_amount) trans_amount_min, max(trans_amount) trans_amount_max,
       sum(used_time) used_time_sum, avg(used_time) used_time_avg, min(used_time) used_time_min, max(used_time) used_time_max,
       PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(trans_type AS STRING), CAST(trans_org_id AS STRING), CAST(result_code AS STRING), CAST(trans_org_name AS STRING), CAST(trans_org_type AS STRING), CAST(province AS STRING), CAST(trans_org_manager AS STRING) ) pk
FROM TABLE(TUMBLE(TABLE dwd_transaction_org_detail, DESCRIPTOR(proc_as_eventtime), INTERVAL '10' SECONDS))
GROUP BY window_start, trans_type, trans_org_id, result_code, trans_org_name, trans_org_type, province, trans_org_manager;

-- 批查询, 查最终结果 
SET 'execution.runtime-mode' = 'batch';
SELECT pk, cnt,trans_amount_sum, used_time_avg, query_time FROM dws_transaction_org_summary_10s ;
SET 'execution.runtime-mode' = 'streaming';


```


### 4.2 简单分组窗口聚合, 写入 dws_transaction_summary_10s
无关联维表, 只对 trans_type , trans_org_id , result_code 分组统计;

```sql

SET 'execution.runtime-mode' = 'streaming';
DROP TABLE IF EXISTS dws_transaction_summary_10s;
CREATE TABLE dws_transaction_summary_10s (
      win_time TIMESTAMP(3), 
      trans_type STRING, 
	  trans_org_id INT, 
	  result_code STRING, 
      cnt BIGINT,
      trans_amount_sum DOUBLE,
      trans_amount_avg DOUBLE,
      trans_amount_min DOUBLE,
      trans_amount_max DOUBLE, 
	  used_time_sum BIGINT, 
	  used_time_avg BIGINT, 
	  used_time_min BIGINT, 
	  used_time_max BIGINT, 
      query_time TIMESTAMP(3),
      pk STRING,
      PRIMARY KEY (pk) NOT ENFORCED,
      WATERMARK FOR win_time AS win_time
);

INSERT INTO dws_transaction_summary_10s 
SELECT window_start, trans_type, trans_org_id, result_code, 
       count(*) cnt, 
	   sum(trans_amount) trans_amount_sum, avg(trans_amount) trans_amount_avg, min(trans_amount) trans_amount_min, max(trans_amount) trans_amount_max, 
	   sum(used_time) used_time_sum, avg(used_time) used_time_avg, min(used_time) used_time_min, max(used_time) used_time_max, 
	   PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(trans_type AS STRING), CAST(trans_org_id AS STRING), CAST(result_code AS STRING)) pk 
FROM TABLE(TUMBLE(TABLE dwd_transaction_detail, DESCRIPTOR(proc_as_eventtime), INTERVAL '10' SECONDS)) 
GROUP BY window_start, trans_type, trans_org_id, result_code ;

SET 'execution.runtime-mode' = 'batch';
select pk, cnt, trans_amount_sum, used_time_avg from dws_transaction_summary_10s;
SET 'execution.runtime-mode' = 'streaming';


```



### 4.3 其他dws 


