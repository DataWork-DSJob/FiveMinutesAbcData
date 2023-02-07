
# Flink 基于Table-Store的实时数仓搭建Demo 

## Demo 需求



准备 Catalog和Db
```sql

CREATE CATALOG table_store_catalog WITH (
  'type'='table-store',
  'warehouse'='file:/tmp/table_store'
);

USE CATALOG table_store_catalog;
show tables;

SET 'execution.checkpointing.interval' = '8 s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

```


### ODS 原始数据导入层 

```sql

-- create a TEMPORARY ods_trade_detail,Table Store Catalog 仅支持默认的[table store]表, 即会用TableStore引擎存储文件系统; 
USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';

-- 1. 模拟解析后的 InTs 开始交易日志
DROP TEMPORARY TABLE IF EXISTS dwd_zq_begin;
CREATE TEMPORARY TABLE dwd_zq_begin (
    k_00 INT,
    quote_begin INT,
    msg_time AS localtimestamp,
    WATERMARK FOR msg_time AS msg_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.k_00.kind' = 'random',
'fields.k_00.min' = '1',
'fields.k_00.max' = '3',
'fields.quote_begin.kind' = 'sequence',
'fields.quote_begin.start' = '1',
'fields.quote_begin.end' = '10001'
);
SELECT * FROM dwd_zq_begin;
-- Ctl + C 退出 动态结果打印


DROP TEMPORARY TABLE IF EXISTS dwd_zq_end;
CREATE TEMPORARY TABLE dwd_zq_end (
    k_00 INT,
    security_id STRING, 
    quote_end INT,
    gateway_start INT, 
    gateway_end INT, 
    msg_time AS localtimestamp,
    WATERMARK FOR msg_time AS msg_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.k_00.kind' = 'random',
'fields.k_00.min' = '1',
'fields.k_00.max' = '3',
'fields.security_id.length' = '2',
'fields.quote_end.kind' = 'sequence',
'fields.quote_end.start' = '2',
'fields.quote_end.end' = '10002',
'fields.gateway_start.kind' = 'sequence',
'fields.gateway_start.start' = '3',
'fields.gateway_start.end' = '10003',
'fields.gateway_end.kind' = 'sequence',
'fields.gateway_end.start' = '5',
'fields.gateway_end.end' = '10005'

);
SELECT * FROM dwd_zq_end;
-- Ctl + C 退出 动态结果打印


```

### DWD 数仓明细层: 每笔交易的明细数据

```sql

USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';


-- begin_table join end_table 

select *
from dwd_zq_begin b inner join dwd_zq_end e on b.k_00 = e.k_00 ;


--- 把 open & close 数据都写入到 同一个表

DROP TABLE IF EXISTS dwd_zq_detail;
CREATE TABLE dwd_zq_detail (
k_00 INT PRIMARY KEY,
security_id STRING,
quote_begin INT,
quote_end INT,
gateway_start INT,
gateway_end INT,
start_ts TIMESTAMP(3),
end_ts TIMESTAMP(3),
update_time AS localtimestamp,
WATERMARK FOR update_time AS update_time
);




INSERT INTO dwd_zq_detail
SELECT
    ods_trade_detail.*,
    PROCTIME() AS op_time
FROM ods_trade_detail;


```


### DIM 维表层

准备基础维表, 这里利用 datagen生成批数据;
```sql

USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';

-- Payment 支付渠道信息维表
DROP TEMPORARY TABLE IF EXISTS dim_payment_info;
CREATE TEMPORARY TABLE dim_payment_info (
payment_id INT PRIMARY KEY,
payment_name STRING,
pay_unicode STRING,
is_online TINYINT,
device_type TINYINT,
update_time TIMESTAMP(3),
WATERMARK FOR update_time AS update_time
) WITH (
'connector' = 'datagen',
'fields.payment_id.kind' = 'sequence',
'fields.payment_id.start' = '50001',
'fields.payment_id.end' = '50020',
'fields.payment_name.length' = '5',
'fields.pay_unicode.length' = '2',
'fields.is_online.min' = '0',
'fields.is_online.max' = '1',
'fields.device_type.min' = '1',
'fields.device_type.max' = '4'
);
SELECT * FROM dim_payment_info;


-- Client & Branch Info, 客户信息, 分行信息 
DROP TEMPORARY TABLE IF EXISTS dim_client_info;
CREATE TEMPORARY TABLE dim_client_info (
client_id INT PRIMARY KEY,
client_name STRING,
branch_id INT,
update_time TIMESTAMP(3),
WATERMARK FOR update_time AS update_time
) WITH (
'connector' = 'datagen',
'fields.client_id.kind' = 'sequence',
'fields.client_id.start' = '2001',
'fields.client_id.end' = '2008',
'fields.client_name.length' = '4',
'fields.branch_id.kind' = 'random',
'fields.branch_id.min' = '101',
'fields.branch_id.max' = '103'
);
SELECT * FROM dim_client_info;


DROP TEMPORARY TABLE IF EXISTS dim_branch_info;
CREATE TEMPORARY TABLE dim_branch_info (
branch_id INT PRIMARY KEY,
branch_name STRING,
update_time TIMESTAMP(3),
WATERMARK FOR update_time AS update_time
) WITH (
'connector' = 'datagen',
'fields.branch_id.kind' = 'sequence',
'fields.branch_id.start' = '101',
'fields.branch_id.end' = '105',
'fields.branch_name.length' = '2'
);
SELECT * FROM dim_branch_info;


```


制作维度大宽表, 存到TableStore中, 方便join, 动态更新

```sql

USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';

DROP TABLE IF EXISTS dim_client_branch ;
CREATE TABLE dim_client_branch (
client_id INT PRIMARY KEY,
client_name STRING,
branch_id INT,
branch_name STRING,
update_time TIMESTAMP(3)
);

INSERT INTO dim_client_branch
SELECT
    c.client_id,c.client_name, c.branch_id, b.branch_name,
    PROCTIME() AS update_time
FROM dim_client_info c INNER JOIN dim_branch_info b 
    ON c.branch_id = b.branch_id AND c.update_time BETWEEN b.update_time - INTERVAL '5' MINUTE AND b.update_time 
;

SELECT * FROM dim_client_branch;
-- Ctl + C 退出 动态结果打印

```

### DWS 维度聚合层

一行交易明细数据(dwd_trade_detail) 包含的维度有
* 事件时间段
* client_id INT  
* client_name STRING
* branch_id INT
* branch_name STRING
* payment_id INT,
* payment_name STRING
* pay_unicode STRING
* is_online TINYINT
* device_type TINYINT
* trade_status INT

客户业务含义和业务需求, 可选用:
* 1min-window 时间段   
* client_id   交易客户ID
* branch_id   分行机构ID
* pay_unicode 支付唯一标识码
* is_online   是否线上交易
* device_type 交易终端类型(微信/PC/APP/支付宝/银联卡)
* trade_status 交易结果状态(-1失败,0成功,1交易中)


按时间维度, client_id, branch_id, payment_id, trade_status 等维度聚合

```sql

USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';

---  1分钟明细聚合结果  ==> join payment + client_branch 信息   ==> 进一步按(新增的)多维度聚合; 
DROP TABLE IF EXISTS dws_trade_summary_1m;
CREATE TABLE dws_trade_summary_1m (
pk STRING PRIMARY KEY,
win_time TIMESTAMP(3),
client_id INT,
branch_id INT,
payment_id INT, 
pay_unicode STRING,
is_online TINYINT,
device_type TINYINT,
trade_status INT,
cnt BIGINT,
query_time TIMESTAMP(3),
WATERMARK FOR win_time AS win_time
);


INSERT INTO dws_trade_summary_1m 
SELECT
    concat_ws('', CAST(client_id AS STRING), CAST(branch_id AS STRING), CAST(payment_id AS STRING), CAST(pay_unicode AS STRING), CAST(is_online AS STRING), CAST(device_type AS STRING), CAST(trade_status AS STRING)) pk,
    window_start, client_id, branch_id, payment_id, pay_unicode, is_online, device_type, trade_status, 
    sum (cnt) cnt,
    PROCTIME() AS query_time
FROM (
         SELECT a.* , p.payment_name, p.pay_unicode, p.is_online, p.device_type, c.branch_id, c.client_name, c.branch_name, c.update_time AS dim_update_time
         FROM (
                  SELECT window_start, client_id, payment_id, trade_status, count(*) cnt
                  FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES))
                  GROUP BY window_start, client_id, payment_id, trade_status
         ) a JOIN dim_payment_info AS p ON a.payment_id = p.payment_id JOIN dim_client_branch AS c ON a.client_id = c.client_id
     )
GROUP BY window_start, client_id, branch_id, payment_id, pay_unicode, is_online, device_type, trade_status
;

SELECT * FROM dws_trade_summary_1m;
-- Ctl + C 退出 动态结果打印


```


### ADS 应用层

--- 应用1 小时级天级 汇总/聚合报表

```sql

USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';

DROP TABLE IF EXISTS ads_trade_summary_1d;
CREATE TABLE ads_trade_summary_1d (
win_time TIMESTAMP(3),
client_id INT,
branch_id INT,
payment_id INT,
is_online TINYINT,
device_type TINYINT,
trade_status INT,
cnt BIGINT,
query_time TIMESTAMP(3)
);

INSERT INTO ads_trade_summary_1d 
SELECT
    window_start AS win_time, 
    client_id, branch_id, payment_id, is_online, device_type, trade_status, 
    SUM(cnt) AS cnt,
    PROCTIME() AS query_time
FROM TABLE(TUMBLE(TABLE dws_trade_summary_1m, DESCRIPTOR(win_time), INTERVAL '1' DAYS))
GROUP BY window_start, client_id, branch_id, payment_id, is_online, device_type, trade_status
;

--- 查询批结果; 最终提供的对外服务, 按批查询; 
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM ads_trade_summary_1d WHERE client_id = 2001;


```


--- 2. 用户进一步关联查询 AdHoc 查询: 查询 客户名(client_id=2001)存于 不同交易环境(线上/线下) 交易失败的次数统计和特征分析;

```sql

SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
--- 查询 客户名(client_id=2001)存于 不同交易环境(线上/线下) 交易失败的次数统计和特征分析; 
WITH user_trade_tmp AS (
select client_id, device_type, sum(cnt) AS cnt 
from dws_trade_summary_1m
where client_id = 2001  and win_time > '2022-03-12' and trade_status = 0
group by client_id, device_type
) 
SELECT a.client_id, c.branch_name, c.client_name, a.device_type , a.cnt as fail_count 
FROM user_trade_tmp a JOIN dim_client_branch AS c ON a.client_id = c.client_id 
;


```


## Flink-Table-Store 功能和特点总结

Table-Store要解决的问题和功能实现
* 各Connector对AppendOnly, Upset, 无主键更新 的SQL语义支持不一, Table-Store要支持Streaming SQL产生的所有实时更新类型。
* 支持SQL Upset更新类型: 包括主键更新、无主键更新和AppendOnly数据。
* 支持的是更快更大吞吐的实时更新。基于LSM Tree实现, 据说读和写性能比Hudi高;
* 为了更加方便, 有效的支持:  离线数仓加速, 大宽表更新, Rollup预聚合, 实时数仓增强 等场景; 

官方定义table-store(v2)的首要功能: 满足Flink SQL对实时数据流的存储的需求
- 作为消息队列, 实时存储和读取实时数据流;
- 表数据实现OLAP可查的功能。
- 支持Batch ETL的写入和大规模Scan。
- 支持Dim Lookup 

Flink-Table-Store 特点分析
* 其本质是支持快速读写的 Connector, 以flink connector的集成和适用;
* 其功能和架构类似 Hudi + Clickhouse, 结合Flink计算能力一起提供了近实时的OLAP分析能力;
* 流批一体,实时数仓和仓湖一体建设正火热发展, 基于Table-Store存储的方案有 " 架构精简, 高效; 上手容易,整合成本低" 特点; 是一个有力的选择;



关键字: 
* Streaming SQL, 流数据处理;
* 实时Upset, 无主键更新;
* 流批一体, 增量计算
	


