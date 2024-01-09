SET 'sql-client.execution.result-mode'='TABLEAU';

CREATE TABLE IF NOT EXISTS dwd_trade_detail (
    t_id BIGINT,
    t_client AS 'Client_A',
    t_time AS localtimestamp,
    t_amount DOUBLE,
    WATERMARK FOR t_time AS t_time - INTERVAL '1' MINUTE
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '2',
      'fields.t_id.kind' = 'sequence',
      'fields.t_id.start' = '1',
      'fields.t_id.end' = '10000',
      'fields.t_amount.kind' = 'random',
      'fields.t_amount.min' = '91',
      'fields.t_amount.max' = '109'
      );

DROP TABLE IF EXISTS dws_trade_summary_10s;
CREATE TABLE IF NOT EXISTS dws_trade_summary_10s (
    window_start TIMESTAMP(3),
    t_client     STRING,
    cnt          BIGINT,
    t_amount_sum DOUBLE,
    PRIMARY KEY (window_start, t_client) NOT ENFORCED,
    WATERMARK FOR window_start AS window_start
) WITH (
     'connector' = 'print'
);

INSERT INTO dws_trade_summary_10s
SELECT
    window_start,
    t_client,
    count(*) cnt,
    SUM(t_amount) AS t_amount_sum
FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(t_time), INTERVAL '10' SECONDS))
GROUP BY window_start, t_client;


