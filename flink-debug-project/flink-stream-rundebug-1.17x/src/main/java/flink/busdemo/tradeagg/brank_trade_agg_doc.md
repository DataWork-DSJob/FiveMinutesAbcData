
# 金融(银行,证券) 交易指标统计

```text
开始数据(5字段): type	trade_id 	client_id	trade_time		offset 
Start,tid1001,cid201,2023-10-04 10:07:22.239,1
Start,tid1002,cid201,2023-10-04 10:07:22.239,2
Start,tid1003,cid201,2023-10-04 10:07:22.239,3


## 结束数据(6字段): type	trade_id 	result_code		trade_amount	trade_time	offset 
### type,trade_id,result_code,trade_amount,trade_time,offset 
Close,tid1001,000,98.9,2023-10-04 10:07:23.768,4
Close,tid1002,000,98.9,2023-10-04 10:07:23.768,5
Close,tid1003,000,98.9,2023-10-04 10:07:23.768,6

```

select 
  trade_id, 
  count(*) as cnt, 
  sum(trade_amount) as sum
from dwd_trade_close
group by trade_id

