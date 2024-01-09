package flink.debug.tableapi;

import flink.debug.FlinkDebugCommon;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class FlinkSQLPaimonExample extends FlinkDebugCommon {

    @Test
    public void testDatagenPaimon() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("", 0323);
        /*
CREATE CATALOG table_store_catalog WITH (
  'type'='table-store',
  'warehouse'='file:/tmp/table_store'
)

USE CATALOG table_store_catalog

SET 'sql-client.execution.result-mode' = 'tableau'


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
)

CREATE TABLE dwd_trade_detail (
    trade_id BIGINT,
    trade_status INT,
    payment_id INT,
    client_id INT,
    trade_time TIMESTAMP(3),
    op_time TIMESTAMP(3),
    PRIMARY KEY (trade_id) NOT ENFORCED,
    WATERMARK FOR trade_time AS trade_time
)

INSERT INTO dwd_trade_detail
SELECT
    ods_trade_detail.*,
    PROCTIME() AS op_time
FROM ods_trade_detail


select * from dwd_trade_detail;

         */

        tableEnv.executeSql("CREATE CATALOG table_store_catalog WITH (\n" +
                "  'type'='paimon',\n" +
                "  'warehouse'='file:/tmp/paimon'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG table_store_catalog");
        tableEnv.executeSql("SET 'sql-client.execution.result-mode' = 'tableau'");


        tableEnv.executeSql("CREATE TEMPORARY TABLE ods_trade_detail (\n" +
                "    trade_id BIGINT,\n" +
                "    trade_status INT,\n" +
                "    payment_id INT,\n" +
                "    client_id INT,\n" +
                "    trade_time AS localtimestamp,\n" +
                "    WATERMARK FOR trade_time AS trade_time\n" +
                ") WITH (\n" +
                "'connector' = 'datagen',\n" +
                "'rows-per-second' = '1',\n" +
                "'fields.trade_id.kind' = 'sequence',\n" +
                "'fields.trade_id.start' = '1',\n" +
                "'fields.trade_id.end' = '50000',\n" +
                "'fields.trade_status.min' = '-1',\n" +
                "'fields.trade_status.max' = '1',\n" +
                "'fields.payment_id.kind' = 'random',\n" +
                "'fields.payment_id.min' = '50001',\n" +
                "'fields.payment_id.max' = '50010',\n" +
                "'fields.client_id.kind' = 'random',\n" +
                "'fields.client_id.min' = '2001',\n" +
                "'fields.client_id.max' = '2006'\n" +
                ")");

        tableEnv.executeSql("DROP TABLE IF EXISTS dwd_trade_detail");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  dwd_trade_detail (\n" +
                "    trade_id BIGINT PRIMARY KEY,\n" +
                "    trade_status INT,\n" +
                "    payment_id INT,\n" +
                "    client_id INT,\n" +
                "    trade_time TIMESTAMP(3),\n" +
                "    op_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR trade_time AS trade_time\n" +
                ")");

        tableEnv.executeSql("INSERT INTO dwd_trade_detail \n" +
                "SELECT\n" +
                "    ods_trade_detail.*,\n" +
                "    PROCTIME() AS op_time\n" +
                "FROM ods_trade_detail");

        tableEnv.executeSql("select * from ods_trade_detail").print();



        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), null);

    }


}
