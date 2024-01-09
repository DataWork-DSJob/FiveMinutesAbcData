package flink.debug.paimon;

import flink.debug.FlinkSqlTableCommon;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Random;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class FlinkSQLPaimonExampleF117 extends FlinkSqlTableCommon {

    protected String getPaimonPath() {
//        String path = "hdfs://bdnode103:9000/tmp/paimon_idea";
        String path = "file:///tmp/paimon_f117";
        return path;
    }

    /**
     * 功能特点:
     *  1. DataStrema to Table
     *  2. Watermark: DataStream.assignTime, Schema.watermark by eventTime: Timestamp
     *  3. from TABLE(TUMBLE(TABLE dwd_trade_detail)) group by window_start
     *
     * @throws Exception
     */
    @Test
    public void testSimplePaimonDemo_TradeWarehouse() throws Exception {
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forMonotonousTimestamps()
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
            public void invoke(Tuple2<Boolean, Row> value, SinkFunction.Context context) throws Exception {
                System.out.println(value.f0 + " -> " + value.f1);
            }
        });

        env.execute();

    }

    @Test
    public void testSimplePaimonDemo_TradeWarehouse_ByStatementSet() throws Exception {
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
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>forMonotonousTimestamps()
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
                "    t_id BIGINT,\n" +
                "    t_client STRING,\n" +
                "    t_time TIMESTAMP(3),\n" +
                "    t_amount DOUBLE,\n" +
                "    WATERMARK FOR t_time AS t_time - INTERVAL '1' MINUTE\n" +
                ")");
        StreamStatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO dwd_trade_detail SELECT * FROM " + temView);

        String dwsSummaryTable = "dws_trade_summary_10s";
        tableEnv.executeSql("DROP TABLE IF EXISTS dws_trade_summary_10s " );
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS dws_trade_summary_10s (\n" +
                "    win_time TIMESTAMP(3),\n" +
                "    t_client     STRING,\n" +
                "    cnt          BIGINT,\n" +
                "    t_amount_sum DOUBLE,\n" +
                "    PRIMARY KEY (win_time, t_client) NOT ENFORCED,\n" +
                "    WATERMARK FOR win_time AS win_time - INTERVAL '1' MINUTE\n" +
                ")");
        statementSet.addInsertSql("INSERT INTO dws_trade_summary_10s\n" +
                "SELECT\n" +
                "    window_start AS win_time,\n" +
                "    t_client,\n" +
                "    count(*) cnt,\n" +
                "    SUM(t_amount) AS t_amount_sum\n" +
                "FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(t_time), INTERVAL '10' SECONDS))\n" +
                "GROUP BY window_start, t_client ");


        tableEnv.executeSql("DROP TABLE IF EXISTS ads_trade_summary_1d " );
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ads_trade_summary_1d (\n" +
                "    t_date       STRING,\n" +
                "    t_client     STRING,\n" +
                "    cnt          BIGINT,\n" +
                "    t_amount_sum    DOUBLE,\n" +
                "    t_amount_avg    DOUBLE,\n" +
                "    query_time   TIMESTAMP(3),\n" +
                "    PRIMARY KEY (t_date, t_client) NOT ENFORCED\n" +
                ") ");
        statementSet.addInsertSql("INSERT INTO ads_trade_summary_1d\n" +
                "SELECT\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd') AS t_date,\n" +
                "    t_client,\n" +
                "    SUM(cnt) AS cnt,\n" +
                "    SUM(t_amount_sum) AS t_amount_sum,\n" +
                "    SUM(t_amount_sum) / SUM(cnt) AS t_amount_avg,\n" +
                "    PROCTIME() AS query_time\n" +
                "FROM TABLE(TUMBLE(TABLE dws_trade_summary_10s, DESCRIPTOR(win_time), INTERVAL '1' DAYS))\n" +
                "GROUP BY window_start, t_client ");



        TableResult execute = statementSet.execute();
        execute.await();


//        Table sqlQueryTable = tableEnv.sqlQuery("SELECT * FROM " + dwsSummaryTable);
//        tableEnv.toRetractStream(sqlQueryTable, Row.class).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
//            @Override
//            public void invoke(Tuple2<Boolean, Row> value, SinkFunction.Context context) throws Exception {
//                System.out.println(value.f0 + " -> " + value.f1);
//            }
//        });
//
//        env.execute();

    }


}
