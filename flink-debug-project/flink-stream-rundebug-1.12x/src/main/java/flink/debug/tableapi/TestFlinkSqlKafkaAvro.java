package flink.debug.tableapi;

import flink.debug.FlinkDebugCommon;
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
public class TestFlinkSqlKafkaAvro extends FlinkDebugCommon {

    @Test
    public void testKafkaSinkAndReaderWithAvro() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());

        /*

SET 'execution.checkpointing.interval' = '10 s';
SET execution.result-mode=tableau;
SET execution.type=streaming;

DROP TEMPORARY TABLE IF EXISTS kafkaprepare_datagen_source;

CREATE TEMPORARY TABLE kafkaprepare_datagen_source (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time AS localtimestamp,
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.trade_id.kind' = 'sequence',
'fields.trade_id.start' = '1',
'fields.trade_id.end' = '10000',
'fields.trade_type.kind' = 'random',
'fields.trade_type.min' = '201',
'fields.trade_type.max' = '206',
'fields.branch_id.kind' = 'random',
'fields.branch_id.min' = '3001',
'fields.branch_id.max' = '3008',
'fields.trade_amount.kind' = 'random',
'fields.trade_amount.min' = '4000',
'fields.trade_amount.max' = '4002',
'fields.result_code.min' = '-1',
'fields.result_code.max' = '1'
)
;
DROP TEMPORARY TABLE IF EXISTS kafkaprepare_kafka_sink;
CREATE TEMPORARY TABLE kafkaprepare_kafka_sink (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time TIMESTAMP(3)
) WITH (
'connector' = 'kafka',
'topic' = 'dwd_trade_detail',
'properties.bootstrap.servers' = '192.168.51.124:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'earliest-offset',
'format' = 'avro'
)
;

INSERT INTO kafkaprepare_kafka_sink SELECT * FROM kafkaprepare_datagen_source
;

DROP TEMPORARY TABLE IF EXISTS kafkaprepare_kafka_sink;
DROP TEMPORARY TABLE IF EXISTS kafkaprepare_datagen_source;


DROP TABLE IF EXISTS dwd_trade_detail;
CREATE TABLE dwd_trade_detail (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time TIMESTAMP(3),
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'kafka',
'topic' = 'dwd_trade_detail',
'properties.bootstrap.servers' = '192.168.51.124:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'latest-offset',
'format' = 'avro'
)
;

SELECT dwd_trade_detail.* , PROCTIME() AS op_time FROM dwd_trade_detail
;


         */

        // 1. datagen 模拟Kafka 数据 kafkaprepare_datagen_source
        tableEnv.executeSql("CREATE TEMPORARY TABLE kafkaprepare_datagen_source (\n" +
                "    trade_id BIGINT,\n" +
                "    trade_type INT,\n" +
                "    branch_id INT,\n" +
                "    trade_amount DOUBLE,\n" +
                "    result_code INT,\n" +
                "    trade_time AS localtimestamp,\n" +
                "    WATERMARK FOR trade_time AS trade_time\n" +
                ") WITH (\n" +
                "'connector' = 'datagen',\n" +
                "'rows-per-second' = '1',\n" +
                "'fields.trade_id.kind' = 'sequence',\n" +
                "'fields.trade_id.start' = '1',\n" +
                "'fields.trade_id.end' = '10000',\n" +
                "'fields.trade_type.kind' = 'random',\n" +
                "'fields.trade_type.min' = '201',\n" +
                "'fields.trade_type.max' = '206',\n" +
                "'fields.branch_id.kind' = 'random',\n" +
                "'fields.branch_id.min' = '3001',\n" +
                "'fields.branch_id.max' = '3008',\n" +
                "'fields.trade_amount.kind' = 'random',\n" +
                "'fields.trade_amount.min' = '4000',\n" +
                "'fields.trade_amount.max' = '4002',\n" +
                "'fields.result_code.min' = '-1',\n" +
                "'fields.result_code.max' = '1'\n" +
                ")");

        // 2. Kafka Sink 建表: kafkaprepare_kafka_sink

        tableEnv.executeSql("CREATE TEMPORARY TABLE kafkaprepare_kafka_sink (\n" +
                "    trade_id BIGINT,\n" +
                "    trade_type INT,\n" +
                "    branch_id INT,\n" +
                "    trade_amount DOUBLE,\n" +
                "    result_code INT,\n" +
                "    trade_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'dwd_trade_detail',\n" +
                "'properties.bootstrap.servers' = '192.168.51.124:9092',\n" +
                "'properties.group.id' = 'flinksql_demo_gid',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'format' = 'avro'\n" +
                ")");

        // 3. 把 DataGen生成数据 写入到 Kafka Sink中
        tableEnv.executeSql("INSERT INTO kafkaprepare_kafka_sink SELECT * FROM kafkaprepare_datagen_source");

        // 4. Kafka Source 建表, 消费Kafka 数据: dwd_trade_detail
        tableEnv.executeSql("CREATE TABLE dwd_trade_detail (\n" +
                "    trade_id BIGINT,\n" +
                "    trade_type INT,\n" +
                "    branch_id INT,\n" +
                "    trade_amount DOUBLE,\n" +
                "    result_code INT,\n" +
                "    trade_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR trade_time AS trade_time\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'dwd_trade_detail',\n" +
                "'properties.bootstrap.servers' = '192.168.51.124:9092',\n" +
                "'properties.group.id' = 'flinksql_demo_gid',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'avro'\n" +
                ")");

        // 5. 展示 Kafka消费的数据
        tableEnv.executeSql("SELECT dwd_trade_detail.* , PROCTIME() AS op_time FROM dwd_trade_detail")
                .print();

        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), null);

    }


    @Test
    public void testGtLogAvroKafkaSql() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());

        /*

SET 'execution.checkpointing.interval' = '10 s';
SET execution.result-mode=tableau;
SET execution.type=streaming;

DROP TABLE IF EXISTS test_avro_data;
CREATE TABLE test_avro_data (
    `logTypeName` STRING,
    `timestamp` STRING,
    `source` STRING,
    `offset` STRING,
    `measures` MAP<STRING NOT NULL, DOUBLE NOT NULL>,
    `dimensions` MAP<STRING NOT NULL, STRING NOT NULL>,
    `normalFields` MAP<STRING NOT NULL, STRING NOT NULL>
) WITH (
'connector' = 'kafka',
'topic' = 'testAvroData',
'properties.bootstrap.servers' = '192.168.51.124:9092',
'properties.group.id' = 'flinksql_demo_gid',
'scan.startup.mode' = 'latest-offset',
'format' = 'avro'
)
;

SELECT test_avro_data.* , PROCTIME() AS op_time FROM test_avro_data;

         */

        tableEnv.executeSql("CREATE TABLE test_avro_data (\n" +
                "    `logTypeName` STRING,\n" +
                "    `timestamp` STRING,\n" +
                "    `source` STRING,\n" +
                "    `offset` STRING,\n" +
                "    `measures` MAP<STRING NOT NULL, DOUBLE NOT NULL>,\n" +
                "    `dimensions` MAP<STRING NOT NULL, STRING NOT NULL>,\n" +
                "    `normalFields` MAP<STRING NOT NULL, STRING NOT NULL>\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'testAvroData',\n" +
                "'properties.bootstrap.servers' = '192.168.51.124:9092',\n" +
                "'properties.group.id' = 'flinksql_demo_gid',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'avro'\n" +
                ")");

        tableEnv.executeSql("SELECT test_avro_data.* , PROCTIME() AS op_time FROM test_avro_data")
                .print();

        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), null);

    }


}
