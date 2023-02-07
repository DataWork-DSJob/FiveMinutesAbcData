
# Flink 基于Table-Store的实时数仓搭建Demo 

ods: 原始数据: 原始日志采集 Kafka日志
dim: 维表数据 	Mysql 各种维度数据 
dwd: 明细层, 清洗后明细数据		日志事件流
dws: (轻度)汇总层, 明细数据 + dim等 聚合 轻度维度聚合数据 维表分钟级轻聚合
ads: 应用层, 基于各应用主题和 业务场景, 最终直接业务适用的表; 重度聚合层; 


## Flink Table-Store 动态表数据湖存储 方案部署

要求flink-1.15及以上版本; 
需要从新项目(apache/flink-table-store)中下载 flink-table-store-dist-0.2.1.jar 第三方包;

集成 flink的table-store功能很简单, 下载dist包后直接copy到 FLINK/lib下面即可; 其功能类似 Hudi; 

```shell
wget https://dlcdn.apache.org/flink/flink-table-store-0.2.1/flink-table-store-dist-0.2.1.jar

cp flink-table-store-dist-0.2.1.jar {FLINK_HOME}/lib

```

关于Flink1.15 升级内容和风险: 依赖和Api变动不小; 

1. 不再支持 scala_2.11, 为确保savepoint,依赖数据等一致性; 从1.15后所有scala版本必需是2.12的;
2. 大部分模块(flink-cep, flink-clients, flink-connects-xx) 都重命名, 去掉了_scale_version的后缀;
3. Java DataSet/-Stream APIs 独立出来, 不依赖scala版本;
4. 默认支持jdk11, 推荐升级jdk11;
5. Table api 变化
    - The previously deprecated methods TableEnvironment.execute, Table.insertInto, TableEnvironment.fromTableSource, TableEnvironment.sqlUpdate, and TableEnvironment.explain have been removed.
    - Please use TableEnvironment.executeSql, TableEnvironment.explainSql, TableEnvironment.createStatementSet, as well as Table.executeInsert, Table.explain and Table.execute and the newly introduces classes TableResult, ResultKind, StatementSet and ExplainDetail.
    - 统一到单一配置类TableConfig
      6.依赖版本升级:
    - 最低Hadoop支持版本升到 2.8.5; Upgrade the minimal supported hadoop version to 2.8.5;
    - 要求zk最低版本3.5以上;
6. 还是其他不少的变动功能, 详解: https://nightlies.apache.org/flink/flink-docs-release-1.15/release-notes/flink-1.15/#summary-of-changed-dependency-names





## Demo 需求
示例Case, 基于银行交易日志流 和 银行客户/支付机构信息表, 构建实时多维报表和数仓分层;
已有数据: 
* 以银行的客户信息(client_info表,基于client_id可查用户名,开户机构等信息), 支付机构信息(payment_info表, 基于payment_id可查 终端,支付方式等) 为维度数据, 
* 各终端发往kafka中心的实时交易明细数据: 本次交易(trade_id)的客户(client_id), 交易方式(payment_id), 交易结果状态(trade_status)

演示的分析需求/功能: 
* 构建分层数仓: ods, dim, dwd, dws, ads 
* ads层实现天级粒度的 多维度(client,branch,payment,tradeStatus等) 实时统计报表; 
* ads层实现: 按客户名(client_id=2001) 查询用户存于不同交易环境(线上/线下)下的 交易失败次数统计, 分析交易失败的特征情况; 


### flink的 sql-client启动和 Catalog和DB准备

```shell
# 安装好flink-1.15以上版本后, cd到flink安装目录; 
cd $FLINK_HOME

# 启动Standalone集群, 用作测试集群(资源)
# 注意,flink-1.15版本启动的Rest & FlinkWeb服务默认监控localhost地址而外网不能访问; 需
# 修改 conf/flink-conf.yaml配置文件为:  rest.bind-address: 0.0.0.0 , 才能外网访问; 
bin/start-cluster.sh

# 启动Flink Client客户段,用于提交sql代码(作业); 
bin/sql-client.sh embedded

# 在flink-sql-client中 输入下面的sql操作语句; 
... 

# 退出 flink-sql-client 
exit;
# 关闭 flink Standalone测试集群; 
bin/stop-cluster.sh

```

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

DROP TEMPORARY TABLE IF EXISTS ods_trade_detail;
CREATE TEMPORARY TABLE ods_trade_detail (
    trade_id BIGINT,
    trade_status INT,
    payment_id INT,
    client_id INT,
    trade_time AS localtimestamp,
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.trade_id.kind' = 'sequence',
'fields.trade_id.start' = '1',
'fields.trade_id.end' = '50000',
'fields.trade_status.min' = '-1',
'fields.trade_status.max' = '1',
'fields.payment_id.kind' = 'random',
'fields.payment_id.min' = '50001',
'fields.payment_id.max' = '50010',
'fields.client_id.kind' = 'random',
'fields.client_id.min' = '2001',
'fields.client_id.max' = '2006'
);

SELECT
    ods_trade_detail.* ,
    PROCTIME() AS op_time
FROM ods_trade_detail;
-- Ctl + C 退出 动态结果打印

```

### DWD 数仓明细层: 每笔交易的明细数据

```sql

USE CATALOG table_store_catalog;
SET 'execution.runtime-mode' = 'streaming';

-- create a table_store table, to store trade_detail 
DROP TABLE IF EXISTS dwd_trade_detail;
CREATE TABLE dwd_trade_detail (
    trade_id BIGINT PRIMARY KEY,
    trade_status INT,
    payment_id INT,
    client_id INT,
    trade_time TIMESTAMP(3),
    op_time TIMESTAMP(3),
    WATERMARK FOR trade_time AS trade_time
);

-- table store requires checkpoint interval in streaming mode

INSERT INTO dwd_trade_detail 
SELECT
    ods_trade_detail.*,
    PROCTIME() AS op_time
FROM ods_trade_detail;

SELECT * FROM dwd_trade_detail;
-- Ctl + C 退出 动态结果打印

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




--- 查询批结果; 最终提供的对外服务, 按批查询; 
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM dws_trade_summary_1m limit 10;

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
	


