
# Flink 基于Table-Store的实时数仓搭建Demo 

[toc]

## Demo 需求

交易日志中的 交易明细数据
```json

{
  "trade_id": "TID001120230315154220",
  "trade_time": "2023-03-15 15:42:20.47",
  "branch_id": "BID0011",
  "trade_amount": 4000.00,
  "trade_type": 102,
  "result_code": "0000"
}

```

需要根据 branch_info维表, 按branch_id关联丰富上 branch相关字段
```json

{
  "branch_id": "BID0011",
  "branch_name": "分支机构11",
  "branch_type": "二级",
  "province": "上海",
  "branch_manager": "张三",
  "branch_mgr_phone": "18608060806",
  "branch_mgr_email": "zhansan@github.com",
  "trade_time": "2023-02-13 15:42:20.47"
}

```




准备 Catalog和Db
```sql

CREATE CATALOG test_mem_catalog WITH (
 'type'='generic_in_memory'
);
USE CATALOG test_mem_catalog;
show tables;

SET 'execution.checkpointing.interval' = '10 s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

```

## 模拟数据: 点击表 + 曝光表 
曝光日志 + 点击日志 => 用户的曝光点击日志

```sql

USE CATALOG test_mem_catalog;

-- 曝光日志数据
DROP TABLE IF EXISTS show_log_table;
CREATE TABLE show_log_table (
    msg_id BIGINT,
    log_id BIGINT,
    show_params STRING,
    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),
    WATERMARK FOR row_time AS row_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.msg_id.kind' = 'sequence',
'fields.msg_id.start' = '1',
'fields.msg_id.end' = '10000',
'fields.show_params.length' = '1',
'fields.log_id.min' = '1',
'fields.log_id.max' = '50'
);

-- 点击日志数据
DROP TABLE IF EXISTS click_log_table;
CREATE TABLE click_log_table (
    msg_id BIGINT, 
    log_id BIGINT, 
    click_params STRING, 
    row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)), 
    WATERMARK FOR row_time AS row_time 
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.click_params.length' = '1',
'fields.msg_id.kind' = 'sequence',
'fields.msg_id.start' = '10001',
'fields.msg_id.end' = '20000',
'fields.log_id.min' = '40',
'fields.log_id.max' = '60'
);
SELECT * FROM click_log_table;

```


## Regular Join 常规表关联

```sql

--  INNER JOIN，条件为 log_id: 用户的曝光 + 点击日志 
SELECT
    DATE_FORMAT(PROCTIME(), 'mm:ss') AS q_time, 
    show_log_table.log_id AS log_key,
    show_log_table.msg_id AS show_msg_id, 
    show_log_table.show_params,
    click_log_table.msg_id AS click_msg_id,
    click_log_table.click_params 
FROM show_log_table
    INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;


-- OutJoin: Left Join, 左边表全输出?  右边表匹配(ON条件,为 log_id相等),则输出;  用户的曝光 + 点击日志 
-- Left Join: 左表新数据(全) + 左表匹配历史数据 + 右表匹配历史数据; 
SELECT
    DATE_FORMAT(PROCTIME(), 'mm:ss') AS q_time,
    show_log_table.log_id AS log_key,
    show_log_table.msg_id AS show_msg_id,
    show_log_table.show_params,
    click_log_table.msg_id AS click_msg_id,
    click_log_table.click_params
FROM show_log_table
    LEFT JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;

-- OutJoin: Right Join,  右边表全输出, 左边表匹配(ON条件,为 log_id相等),则输出; 
SELECT
    DATE_FORMAT(PROCTIME(), 'mm:ss') AS q_time,
    show_log_table.log_id AS log_key,
    show_log_table.msg_id AS show_msg_id,
    show_log_table.show_params,
    click_log_table.msg_id AS click_msg_id,
    click_log_table.click_params
FROM show_log_table
    RIGHT JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;

--- OutJoin: Full Outer Join , 左表新数据 + 右表新数据 + 左表匹配的历史数据 + 右表匹配的历史数据; 
SELECT
  DATE_FORMAT(PROCTIME(), 'mm:ss') AS q_time,
  show_log_table.log_id AS log_key,
  show_log_table.msg_id AS show_msg_id,
  show_log_table.show_params,
  click_log_table.msg_id AS click_msg_id,
  click_log_table.click_params
FROM show_log_table
    FULL OUTER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id;

```

![flinksql_join_regular_inner.png](images/flinksql_join_regular_inner.png)
Rugular Join 特点: 
* 历史数据无止境的关联(缓存); Regular没有缓存清理策略; 
* Inner Join, 只输出 完全匹配(On条件)的行;  新数据中左表右表完全匹配的 + 左表匹配历史数据 + 右表匹配历史数据;


![LeftJoin.png](images/flinksql_regularjoin_leftJoin.png)
LeftJoin(Regular): 左表新数据(全) + 左表匹配历史数据 + 右表匹配历史数据;

![FullOutJoin.png](images/flinksql_regularjoin_fullOutJoin.png)
Full-Out-Join: 左表新数据 + 右表新数据 + 左表匹配的历史数据 + 右表匹配的历史数据; 



## Window Interval Join 时间间隔管理

Interval Join 可以让一条流去 Join 另一条流中前后一段时间内的数据
Interval Join 可用于消灭回撤流的。?
Time-Windowed Join 利用窗口给两个输入表设定一个 Join 的时间界限，超出时间范围的数据则对 JOIN 不可见并可以被清理掉。
关于Time-Windowed-Join, 
* 给两个输入表设置时间界限, 超出范围(过期)数据就可用丢弃而不参与Join; 
* 可用是ProcessTime或EventTime, 系统时间就自动划分 Join 的时间窗口并定时清理数据; EventTime基于水位;
* compared to the regular join, interval join only supports append-only tables with time attributes. 
* Since time attributes are quasi-monotonic increasing, Flink can remove old values from its state without affecting the correctness of the result.
* 实时 Interval Join 可以不是 等值 join。等值 join 和 非等值 join 区别在于，等值 join 数据 shuffle 策略是 Hash，会按照 Join on 中的等值条件作为 id 发往对应的下游；
* 非等值 join 数据 shuffle 策略是 Global，所有数据发往一个并发，然后将满足条件的数据进行关联输出

```sql

--  IntervalWindowJoin: INNER JOIN， clickTime > showTime > clickTime - 2min
-- 实际案例: 曝光日志关联点击日志筛选既有曝光又有点击的数据，条件是曝光关联之后发生 4 小时之内的点击，并且补充点击的扩展参数（show inner interval click）
-- 给左表: show表 设置了 : showTime > clickTime - 2min  下界;  
-- 给右表: click表 设置了 : clickTime > showTime (watermark?) 下界; 

SELECT
  DATE_FORMAT(PROCTIME(), 'mm:ss') AS q_time,
  show_log_table.log_id AS log_key,
  show_log_table.msg_id AS show_msg_id,
  click_log_table.msg_id AS click_msg_id,
  timestampDiff(SECOND, show_log_table.row_time, click_log_table.row_time) AS interval_120s,
  DATE_FORMAT(show_log_table.row_time, 'mm:ss.SSS') AS show_time,
  DATE_FORMAT(click_log_table.row_time, 'mm:ss.SSS') AS click_time
FROM show_log_table INNER JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id
  AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '2' MINUTE AND click_log_table.row_time;
-- 测试时, 可分别设置 时间间隔 30s -> 2min -> 1hour


--  IntervalWindowJoin: Left Join，  clickTime + 2 > showTime > clientTime - 2 ; 
-- showTime > clientTime - 40 sec; 
-- clickTime > showTime - 20 sec; 

SELECT
  DATE_FORMAT(PROCTIME(), 'mm:ss') AS q_time,
  show_log_table.log_id AS log_key,
  show_log_table.msg_id AS show_msg_id,
  click_log_table.msg_id AS click_msg_id,
  timestampDiff(SECOND, show_log_table.row_time, click_log_table.row_time) AS interval_120s,
  DATE_FORMAT(show_log_table.row_time, 'mm:ss.SSS') AS show_time,
  DATE_FORMAT(click_log_table.row_time, 'mm:ss.SSS') AS click_time
FROM show_log_table LEFT JOIN click_log_table ON show_log_table.log_id = click_log_table.log_id
  AND show_log_table.row_time BETWEEN click_log_table.row_time - INTERVAL '40' SECOND AND click_log_table.row_time + INTERVAL '20' SECOND;



```

![img.png](images/fsql_intervaljoin_innerJoin2m.png)
IntervalJoin_InnerJoin: 新数据( 右表 + 左表  ) + 左表2分钟内历史数据 右表(看Interval条件)


## Temporal Table Join 动态拉链表关联Join

Temporal joins 基本
* Temporal joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row’s relevant version in the versioned table (right input/build side).
  - 左表(left input/ probe site/测量点) , 明细表, 一般是业务数据流; 典型的数据流远大于右边(维度表)
  - 右表: versioned table (right input/build side), 时态表, 版本表, 拉链快照表, 一般是纬度表的 changelog; 
  - 根据时态表是否可以追踪自身的历史版本与否，时态表可以分为 版本表 和 普通表
* Temporal Table Join 类似于 Hash Join，将输入分为左表Probe Table  和 右表 Build Table ;
* Build Table 是一个基于 append-only 数据流的带时间版本的视图，所以又称为 Temporal Table。Temporal Table 要求定义一个主键和用于版本化的字段（通常就是 Event Time 时间字段），以反映记录在不同时间的内容。


```sql

-- 1. 定义一个输入订单表
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id    BIGINT,
    price       DECIMAL(16,2),
    currency    INT,
    order_time  AS CAST(CURRENT_TIMESTAMP as TIMESTAMP(3)),
    WATERMARK FOR order_time AS order_time
) WITH (
  'connector' = 'datagen', 
  'rows-per-second' = '2', 
  'fields.order_id.kind' = 'sequence',
  'fields.order_id.start' = '1',
  'fields.order_id.end' = '10000', 
  'fields.price.min' = '100', 
  'fields.price.max' = '102', 
  'fields.currency.min' = '1', 
  'fields.currency.max' = '8' 
);

-- 2. 定义一个汇率 versioned 表，其中 versioned 表的概念下文会介绍到
DROP TABLE IF EXISTS currency_rates;
CREATE TABLE currency_rates (
    cr_id INT, 
    currency INT, 
    conversion_rate FLOAT, 
    update_time AS CAST(CURRENT_TIMESTAMP as TIMESTAMP(3)), 
    WATERMARK FOR update_time AS update_time, 
    PRIMARY KEY(currency) NOT ENFORCED
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1',
    'fields.cr_id.kind' = 'sequence',
    'fields.cr_id.start' = '1',
    'fields.cr_id.end' = '10000',
    'fields.currency.min' = '1',
    'fields.currency.max' = '30',
    'fields.conversion_rate.min' = '6.8',
    'fields.conversion_rate.max' = '8.2'
);
SELECT * FROM currency_rates;

-- 3. Temporal Join, 暂时的 动态的 多版本的

SELECT
  order_id, 
  orders.currency, 
  conversion_rate, 
  cr_id, 
  timestampDiff(SECOND, update_time, order_time) AS timediff_sec, 
  order_time 
FROM orders 
    LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time 
    ON orders.currency = currency_rates.currency 
;

```




## Lookup Join 

使用曝光用户日志流（show_log）关联用户画像维表（user_profile）关联到用户的维度之后，提供给下游计算分性别，年龄段的曝光用户数使用。
* The lookup join uses the above Processing Time Temporal Join syntax with the right table to be backed by a lookup source connector
* 左表 处理时间(ProcessTime)事实表, 右表若为 lookup源算子(LookupTableSource接口) 算子, 使用FOR SYSTEM_TIME AS OF 作为TemporalJoin方式Join; 

```sql

-- 曝光用户日志流（show_log）数据（数据存储在 kafka 中）
DROP TABLE IF EXISTS show_log;
CREATE TABLE show_log (
    log_id BIGINT,
    `timestamp` AS CAST(CURRENT_TIMESTAMP as TIMESTAMP(3)),
    user_id INT, 
    proctime AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '1',
  'fields.user_id.min' = '1001',
  'fields.user_id.max' = '1009',
  'fields.log_id.min' = '1',
  'fields.log_id.max' = '10'
);

SELECT * FROM show_log;

--- 这里需要 lookup connector, 像 jdbc, hbase, hive等; 另外, 网上有Hive的; 
-- flink-table-store 目前尚未实现 LookupTableSource 接口, 计划v0.3版实现; 

DROP TABLE IF EXISTS user_profile;
CREATE TABLE user_profile (
    user_id INT,
    age INT,
    user_info STRING, 
    PRIMARY KEY (user_id) NOT ENFORCED 
) WITH (
    'connector' = 'datagen', 
    'rows-per-second' = '1', 
    'fields.user_id.min' = '1001',
    'fields.user_id.max' = '1019',
    'fields.age.min' = '18', 
    'fields.age.max' = '65',
    'fields.user_info.length' = '5'
    );
SELECT * FROM user_profile;


-- lookup join 的 query 逻辑
SELECT s.log_id as log_id
     , s.`timestamp` as `timestamp`
     , s.user_id as user_id
     , s.proctime as proctime
     , u.sex as sex
     , u.age as age
FROM show_log AS s 
    LEFT JOIN user_profile FOR SYSTEM_TIME AS OF s.proctime AS u
    ON s.user_id = u.user_id


```





## 关于Stream SQL 种的Join比较 

实时流, Streaming SQL（面向无界数据集的 SQL）无法缓存所有数据, 也无法对数据集排序;
设置一个缓存剔除策略将不必要的历史数据及时清理是必然策略; 关键在于缓存剔除策略如何实现，这也是 Flink SQL 提供的三种 Join 的主要区别。
* Regular Join 是最为基础的没有缓存剔除策略的 Join。Regular Join 中两个表的输入和更新都会对全局可见，影响之后所有的 Join 结果。
  - 因为历史数据不会被清理，所以 Regular Join 允许对输入表进行任意种类的更新操作（insert、update、delete）。然而因为资源问题 Regular Join 通常是不可持续的，一般只用做有界数据流的 Join。
* 

而 Nested-loop Join 和 Hash Join 经过一定的改良则可以满足实时 SQL 的要求。 Nested Join 在实时 Streaming SQL 的基础实现


