package flink.debug.paimon.bak;

import flink.debug.FlinkSqlTableCommon;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Properties;
import java.util.Random;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class FlinkSQLPaimonExampleF117Bak extends FlinkSqlTableCommon {

    @Override
    protected String getBootstrapServers() {
        return "192.168.51.112:9092";
    }

    protected String getPaimonPath() {
        String path = "hdfs://bdnode103:9000/tmp/paimon_idea";
//        String path = "file:///tmp/paimon_f117";
        return path;
    }


    @Test
    public void testKafkaPaimonWinAggKafka() throws Exception {
        String bootstrapServers = getBootstrapServers();

        StreamExecutionEnvironment sEnv = getStreamEnv();
        sEnv.enableCheckpointing(1000 * 3);
        TableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);

        // Catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+getPaimonPath()+"')");
        tableEnv.executeSql("USE CATALOG paimon");

        // 1. 消费Kafka Odw数据
        tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS ods_trade_csv");
        tableEnv.executeSql("CREATE TEMPORARY TABLE ods_trade_csv (\n" +
                "    record_id BIGINT,\n" +
                "    group_key STRING,\n" +
                "    time_sec BIGINT,\n" +
                "    amount DOUBLE, \n" +
                "    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec)), \n" +
                "    WATERMARK FOR event_time AS event_time\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'ods_trade_csv',\n" +
                "'properties.bootstrap.servers' = '"+ bootstrapServers +"',\n" +
                "'properties.group.id' = 'testGroupId_winIdea',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");

        // 2. 在 Paimon 中建立 Dws 层,
        tableEnv.executeSql("DROP TABLE IF EXISTS dws_trade_summary_10s");
        tableEnv.executeSql("CREATE TABLE dws_trade_summary_10s (\n" +
                "    win_time TIMESTAMP(3),\n" +
                "    group_key STRING,\n" +
                "    cnt BIGINT,\n" +
                "    amount_sum DOUBLE,\n" +
                "    query_time TIMESTAMP(3),\n" +
                "    pk STRING,\n" +
                "    PRIMARY KEY (`pk`) NOT ENFORCED,\n" +
                "    WATERMARK FOR win_time AS win_time\n" +
                ")");


        // 3. 查询Kafka Ods数据, 进行窗口聚合后, 写出到 Paimon Dws 层中;
//        tableEnv.executeSql("SELECT window_start, group_key, count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time, concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)) GROUP BY window_start, group_key\n").print();
        tableEnv.executeSql("INSERT INTO dws_trade_summary_10s\n" +
                "SELECT window_start, group_key,\n" +
                "       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,\n" +
                "       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk\n" +
                "FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))\n" +
                "GROUP BY window_start, group_key");


        // 4. 查看: 分为流查询, 批查询
        tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS dws_trade_summary_10s_kafka");
        tableEnv.executeSql("CREATE TEMPORARY TABLE dws_trade_summary_10s_kafka (\n" +
                "    win_time TIMESTAMP(3),\n" +
                "    group_key STRING,\n" +
                "    cnt BIGINT,\n" +
                "    amount_sum DOUBLE,\n" +
                "    query_time TIMESTAMP(3),\n" +
                "    pk STRING,\n" +
                "    PRIMARY KEY (`pk`) NOT ENFORCED,\n" +
                "    WATERMARK FOR win_time AS win_time\n" +
                ")\n" +
                "WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic' = 'dws_trade_summary_10s_kafka',\n" +
                "    'properties.bootstrap.servers' = '"+bootstrapServers+"',\n" +
                "    'properties.group.id' = 'flinksql_demo_gid',\n" +
                "    'key.format' = 'json',\n" +
                "    'key.json.ignore-parse-errors' = 'true',\n" +
                "    'value.format' = 'json',\n" +
                "    'value.json.fail-on-missing-field' = 'false',\n" +
                "    'value.fields-include' = 'EXCEPT_KEY'\n" +
                " )");
        tableEnv.executeSql("INSERT INTO dws_trade_summary_10s_kafka SELECT * FROM dws_trade_summary_10s");

        tableEnv.executeSql("SELECT * FROM dws_trade_summary_10s_kafka")
                .print();


    }


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
    @Test
    public void testDatagenPaimon() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("", 0323);

        tableEnv.executeSql("CREATE CATALOG table_store_catalog WITH (\n" +
                "  'type'='paimon',\n" +
                "  'warehouse'='file:/tmp/paimon'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG table_store_catalog");

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
                "    trade_id BIGINT,\n" +
                "    trade_status INT,\n" +
                "    payment_id INT,\n" +
                "    client_id INT,\n" +
                "    trade_time TIMESTAMP(3),\n" +
                "    op_time TIMESTAMP(3),\n" +
                "    PRIMARY KEY (trade_id) NOT ENFORCED,\n" +
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

    @Test
    public void testKafkaTableTumbleWinAgg() throws Exception {
        String bootstrapServers = getBootstrapServers();
        StreamExecutionEnvironment sEnv = getStreamEnv();
        sEnv.enableCheckpointing(1000 * 2);
        TableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);

        // Catalog
        tableEnv.executeSql("CREATE CATALOG test_mem_catalog WITH ('type'='generic_in_memory')");
        tableEnv.executeSql("USE CATALOG test_mem_catalog");

        // 1. 消费Kafka Odw数据
        tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS ods_trade_csv");
        tableEnv.executeSql("CREATE TEMPORARY TABLE ods_trade_csv (\n" +
                "    record_id BIGINT,\n" +
                "    group_key STRING,\n" +
                "    time_sec BIGINT,\n" +
                "    amount DOUBLE, \n" +
                "    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec)), \n" +
                "    WATERMARK FOR event_time AS event_time\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'ods_trade_csv',\n" +
                "'properties.bootstrap.servers' = '"+bootstrapServers+"',\n" +
                "'properties.group.id' = 'testGroupId_winIdea',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");

        // 2. 在 Paimon 中建立 Dws 层,
        tableEnv.executeSql("DROP TABLE IF EXISTS dws_trade_summary_10s");
        tableEnv.executeSql("CREATE VIEW dws_trade_summary_10s AS\n" +
                "SELECT window_start AS win_time, group_key,\n" +
                "       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,\n" +
                "       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk\n" +
                "FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))\n" +
                "GROUP BY window_start, group_key");

        tableEnv.executeSql("SELECT win_time, cnt, amount_sum, query_time FROM dws_trade_summary_10s").print();


    }

    @Test
    public void testKafkaTumbleStartWinAggByF117() throws Exception {
        runSqlKafkaTumbleStartWinAgg();

    }

    @Test
    public void testStreamKafka2PaimonSql2KafkaStream() throws Exception {
        String topic = "ods_trade_csv";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getSimpleName());

        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaProps);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafka);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[] {Types.LONG,Types.STRING, Types.LONG, Types.DOUBLE},
                new String[] {"record_id", "group_key", "time_sec", "amount"});
        SingleOutputStreamOperator<Row> dataStream = kafkaDataStream.map(new RichMapFunction<String, Row>() {
            @Override
            public Row map(String str) throws Exception {
                String[] split = str.split(",");
                Row row = new Row(RowKind.INSERT, 4);
                row.setField(0, Long.parseLong(split[0].trim()));
                row.setField(1, split[1]);
                row.setField(2, Long.parseLong(split[2].trim()) );
                row.setField(3, Double.parseDouble(split[3].trim()) );
                return row;
            }
        }, rowTypeInfo);

        Schema schema = Schema.newBuilder()
                .column("record_id", DataTypes.BIGINT())
                .column("group_key", DataTypes.STRING())
                .column("time_sec", DataTypes.BIGINT())
                .column("amount", DataTypes.DOUBLE())
                .columnByExpression("event_time", "TO_TIMESTAMP(FROM_UNIXTIME(time_sec))")
                .watermark("event_time", "event_time - INTERVAL '10' SECOND ")
                .build();

        Table table = tableEnv.fromChangelogStream(dataStream, schema);

        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+getPaimonPath()+"')");
        tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("ods_trade_csv", table);

        // insert into paimon table from your data stream table

        tableEnv.executeSql("DROP TABLE IF EXISTS dws_trade_summary_10s");
        tableEnv.executeSql("CREATE TABLE dws_trade_summary_10s (\n" +
                "    win_time TIMESTAMP(3),\n" +
                "    group_key STRING,\n" +
                "    cnt BIGINT,\n" +
                "    amount_sum DOUBLE,\n" +
                "    query_time TIMESTAMP(3),\n" +
                "    pk STRING,\n" +
                "    PRIMARY KEY (`pk`) NOT ENFORCED,\n" +
                "    WATERMARK FOR win_time AS win_time\n" +
                ")");

        tableEnv.executeSql("INSERT INTO dws_trade_summary_10s\n" +
                "SELECT window_start, group_key,\n" +
                "       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,\n" +
                "       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk\n" +
                "FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))\n" +
                "GROUP BY window_start, group_key");

        Table sinkTable = tableEnv.sqlQuery("SELECT * FROM dws_trade_summary_10s");
        DataStream<Row> sinkDataStream = tableEnv.toChangelogStream(sinkTable);

        DataStreamSink<Row> result = sinkDataStream.addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row value, Context context) throws Exception {
                System.out.println(value.getKind() + " => " + value);

            }
        });

        env.execute(this.getClass().getSimpleName());


    }

    @Test
    public void testMyStream2Paimon2PrintStream() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // create a changelog DataStream
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.LONG},
                new String[]{"trade_id", "client_id", "trade_amount", "offset"}
        );
        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                String[][] clientTradeIdArray = {
                        {"Alice", "tid_alice"},
                        {"Bob", "tid_bob"},
                        {"Lily", "tid_lily"},
                };
                Random random = new Random();
                for (long i = 0; i < 1000; i++) {
                    String[] clientTid = clientTradeIdArray[random.nextInt(clientTradeIdArray.length)];
                    ctx.collect(Row.ofKind(RowKind.INSERT, clientTid[1] + (i/10*10), clientTid[0], 50 + random.nextInt(45), i));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() { }
        }, rowTypeInfo).name("myTradeDetailStreamSrc");
        Table table = tableEnv.fromChangelogStream(dataStream);

        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+ getPaimonPath() +"')");
        tableEnv.executeSql("USE CATALOG paimon");
//        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.createTemporaryView("InputTable", table);

        /*
create table dws_trade_paimon (
  trade_id STRING,
  cnt BIGINT,
  ta_sum DOUBLE,
  PRIMARY KEY (`trade_id`) NOT ENFORCED
);
         */
        tableEnv.executeSql("drop table if exists dws_trade_paimon");
        tableEnv.executeSql("create table dws_trade_paimon (\n" +
                "  trade_id STRING,\n" +
                "  client_id STRING,\n" +
                "  cnt BIGINT,\n" +
                "  ta_sum DOUBLE,\n" +
                "  PRIMARY KEY (`trade_id`, `client_id`) NOT ENFORCED\n" +
                ")");
        tableEnv.executeSql("insert into dws_trade_paimon SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM InputTable GROUP BY trade_id, client_id");

//        Table sqlQueryTable = tableEnv.sqlQuery("SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM InputTable GROUP BY trade_id, client_id");
        Table sqlQueryTable = tableEnv.sqlQuery("SELECT * FROM dws_trade_paimon");
        tableEnv.toRetractStream(sqlQueryTable, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println(value.f0 + " -> " + value.f1);
            }
        });

        env.execute();

    }

    @Test
    public void testMyStream2PaimonDwd2PaimonDws2PrintStream() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // create a changelog DataStream
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.LONG},
                new String[]{"trade_id", "client_id", "trade_amount", "msg_offset"}
        );
        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                String[][] clientTradeIdArray = {
                        {"Alice", "tid_alice"},
                        {"Bob", "tid_bob"},
                        {"Lily", "tid_lily"},
                };
                Random random = new Random();
                long lastPrintSec = System.currentTimeMillis()/1000;
                final int batchSize = 100000;
                long count = 0;
                while (true) {
                    for (long i = 0; i < batchSize; i++) {
                        String[] clientTid = clientTradeIdArray[random.nextInt(clientTradeIdArray.length)];
                        ctx.collect(Row.ofKind(RowKind.INSERT, clientTid[1] + (i/10*10), clientTid[0], 50 + random.nextInt(45), i));
//                    Thread.sleep(1000);
                    }
                    long curSec = System.currentTimeMillis()/1000;
                    count += batchSize;
                    if ((curSec % 2 == 0 || curSec % 5 == 0 ) && curSec - lastPrintSec > 0) {
                        double qpsPerSec = count / (curSec - lastPrintSec);
                        System.out.println("Qps: " + (qpsPerSec /10000) + " w/sec (" + qpsPerSec  + ")");
                        lastPrintSec = curSec;
                        count = 0;
                    }
                }
            }
            @Override
            public void cancel() { }
        }, rowTypeInfo).name("myTradeDetailStreamSrc");
        Table table = tableEnv.fromChangelogStream(dataStream);

        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+ getPaimonPath() +"')");
        tableEnv.executeSql("USE CATALOG paimon");
//        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.executeSql("drop table if exists dwd_trade_paimon");
        tableEnv.executeSql("create table IF NOT EXISTS dwd_trade_paimon (\n" +
                "  trade_id STRING,\n" +
                "  client_id STRING,\n" +
                "  trade_amount INT,\n" +
                "  `msg_offset` BIGINT\n" +
                ") ");
        tableEnv.executeSql("INSERT INTO dwd_trade_paimon SELECT * FROM InputTable");

        /*
create table dws_trade_paimon (
  trade_id STRING,
  cnt BIGINT,
  ta_sum DOUBLE,
  PRIMARY KEY (`trade_id`) NOT ENFORCED
);
         */
        tableEnv.executeSql("drop table if exists dws_trade_paimon");
        tableEnv.executeSql("create table dws_trade_paimon (\n" +
                "  trade_id STRING,\n" +
                "  client_id STRING,\n" +
                "  cnt BIGINT,\n" +
                "  ta_sum DOUBLE,\n" +
                "  PRIMARY KEY (`trade_id`, `client_id`) NOT ENFORCED\n" +
                ")");
        tableEnv.executeSql("insert into dws_trade_paimon SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM dwd_trade_paimon GROUP BY trade_id, client_id");

//        Table sqlQueryTable = tableEnv.sqlQuery("SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM InputTable GROUP BY trade_id, client_id");
        Table sqlQueryTable = tableEnv.sqlQuery("SELECT * FROM dws_trade_paimon");
        tableEnv.toRetractStream(sqlQueryTable, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                if (System.currentTimeMillis() %500 == 0 && LocalTime.now().getSecond() % 5 == 0) {
                    System.out.println(value.f0 + " -> " + value.f1);
                }
            }
        });

        env.execute();

    }



    @Test
    public void testReadPaimon() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().inBatchMode().build()
        );
        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='" + getPaimonPath() + "')");
        tableEnv.executeSql("USE CATALOG paimon");
        // convert to DataStream
        while (true) {
            tableEnv.executeSql("SELECT count(*) cnt FROM dwd_trade_paimon").print();
            Thread.sleep(1000 * 3);
        }

    }

    @Test
    public void testMyStream2Paimon() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // create a changelog DataStream
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.LONG},
                new String[]{"trade_id", "client_id", "trade_amount", "offset"}
        );
        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                String[][] clientTradeIdArray = {
                        {"Alice", "tid_alice"},
                        {"Bob", "tid_bob"},
                        {"Lily", "tid_lily"},
                };
                Random random = new Random();
                for (long i = 0; i < 1000; i++) {
                    String[] clientTid = clientTradeIdArray[random.nextInt(clientTradeIdArray.length)];
                    ctx.collect(Row.ofKind(RowKind.INSERT, clientTid[1] + (i/10*10), clientTid[0], 50 + random.nextInt(45), i));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() { }
        }, rowTypeInfo).name("myTradeDetailStreamSrc");
        Table table = tableEnv.fromChangelogStream(dataStream);

        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+ getPaimonPath() +"')");
        tableEnv.executeSql("USE CATALOG paimon");
//        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.createTemporaryView("InputTable", table);

        /*
create table dws_trade_paimon (
  trade_id STRING,
  cnt BIGINT,
  ta_sum DOUBLE,
  PRIMARY KEY (`trade_id`) NOT ENFORCED
);
         */
        tableEnv.executeSql("drop table if exists dws_trade_paimon");
        tableEnv.executeSql("create table dws_trade_paimon (\n" +
                "  trade_id STRING,\n" +
                "  client_id STRING,\n" +
                "  cnt BIGINT,\n" +
                "  ta_sum DOUBLE,\n" +
                "  PRIMARY KEY (`trade_id`, `client_id`) NOT ENFORCED\n" +
                ")");

        TableResult tableResult = tableEnv.executeSql("insert into dws_trade_paimon SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM InputTable GROUP BY trade_id, client_id");//                .await()
//        tableResult.await();
//        tableResult.await();
//        String insertSql = "insert into dws_trade_paimon SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM " + table + " GROUP BY trade_id, client_id";
//        tableEnv.executeSql(insertSql);

//        Table sqlQueryTable = tableEnv.sqlQuery("SELECT trade_id, client_id, count(*) cnt, SUM(trade_amount) AS ta_sum FROM InputTable GROUP BY trade_id, client_id");
//        Table sqlQueryTable = tableEnv.sqlQuery("SELECT * FROM dws_trade_paimon");
//        tableEnv.toRetractStream(sqlQueryTable, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
//            @Override
//            public void invoke(Tuple2<Boolean, Row> value, SinkFunction.Context context) throws Exception {
//                System.out.println(value.f0 + " -> " + value.f1);
//            }
//        });
        Thread.sleep(3000);


        env.execute();

    }

    @Test
    public void testMyStream2PaimonDwd2PaimonDws2PrintStream_AssignWm() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.LONG, Types.STRING, Types.SQL_TIMESTAMP, Types.DOUBLE},
                new String[]{"t_id", "t_client", "t_time", "t_amount"}
        );
        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                String[] clients = {"Alice", "Bob", "Lily"};
                Random random = new Random();
                final int batchSize = 1000000;
                for (long i = 0; i < batchSize; i++) {
                    double tAmount = 10 + random.nextInt(90)  + random.nextDouble();
                    ctx.collect(Row.of(
                            i,
                            clients[random.nextInt(clients.length)],
                            System.currentTimeMillis(),
                            tAmount
                    ));
                    Thread.sleep(500);
                }
            }
            @Override
            public void cancel() { }
        }).name("myTradeSource")
                .flatMap(new FlatMapFunction<Row, Row>() {
                    @Override
                    public void flatMap(Row row, Collector<Row> collector) throws Exception {
                        Object timeField = row.getField(2);
                        if (null != timeField && timeField instanceof Long) {
                            Timestamp timestamp = new Timestamp((long) timeField);
                            row.setField(2, timestamp);
                            collector.collect(row);
                        }
                    }
                }, rowTypeInfo)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forMonotonousTimestamps()
                                .withTimestampAssigner((ctx) -> (element, recordTimestamp) -> {
                                    Timestamp tsValue = (Timestamp) element.getField(2);
                                    return tsValue.getTime();
                                }))
        ;

        Schema schema = Schema.newBuilder()
                .column("t_id", DataTypes.BIGINT())
                .column("t_client", DataTypes.STRING())
                .column("t_time", DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class))
                .column("t_amount", DataTypes.DOUBLE())
                .watermark("t_time", "t_time - INTERVAL '10' SECOND ")
                .build();
        Table table = tableEnv.fromChangelogStream(dataStream, schema);

        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+ getPaimonPath() +"')");
        tableEnv.executeSql("USE CATALOG paimon");
        String temView = "InputTable";
        tableEnv.createTemporaryView(temView, table);

        String dwdDetailTable = "dwd_trade_detail";
        tableEnv.executeSql("DROP TABLE IF EXISTS " + dwdDetailTable);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dwd_trade_detail (\n" +
                "    t_id BIGINT, \n" +
                "    t_client STRING,\n" +
                "    t_time TIMESTAMP(3),\n" +
                "    t_amount DOUBLE,\n" +
                "    WATERMARK FOR t_time AS t_time - INTERVAL '1' MINUTE\n" +
                ")");
        tableEnv.executeSql("INSERT INTO "+ dwdDetailTable + " SELECT * FROM " + temView);

        String dwsSummaryTable = "dws_trade_summary_10s";
        tableEnv.executeSql("drop table if exists " + dwsSummaryTable);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dws_trade_summary_10s (\n" +
                "    win_start TIMESTAMP(3),\n" +
                "    t_client     STRING,\n" +
                "    cnt          BIGINT,\n" +
                "    t_amount_sum DOUBLE,\n" +
                "    PRIMARY KEY (win_start, t_client) NOT ENFORCED,\n" +
                "    WATERMARK FOR win_start AS win_start\n" +
                ")");
        tableEnv.executeSql("INSERT INTO dws_trade_summary_10s \n" +
                "SELECT \n" +
                "    window_start,  \n" +
                "    t_client, \n" +
                "    count(*) cnt, \n" +
                "    SUM(t_amount) AS t_amount_sum \n" +
                "FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(t_time), INTERVAL '10' SECONDS)) \n" +
                "GROUP BY window_start, t_client ");

        Table sqlQueryTable = tableEnv.sqlQuery("SELECT * FROM " + dwsSummaryTable);
        tableEnv.toRetractStream(sqlQueryTable, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println(value.f0 + " -> " + value.f1);
            }
        });

        env.execute();

    }


}
