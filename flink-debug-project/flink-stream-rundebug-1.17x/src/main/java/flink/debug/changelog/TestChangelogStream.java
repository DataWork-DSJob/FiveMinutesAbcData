package flink.debug.changelog;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.row;

public class TestChangelogStream extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.51.124:9092";

    private static Either<Row, Row> input(RowKind kind, Object... fields) {
        return Either.Left(Row.ofKind(kind, fields));
    }

    private static Row[] getInput(List<Either<Row, Row>> inputOrOutput) {
        return inputOrOutput.stream().filter(Either::isLeft).map(Either::left).toArray(Row[]::new);
    }

    private static Either<Row, Row> output(RowKind kind, Object... fields) {
        return Either.Right(Row.ofKind(kind, fields));
    }

    private static Row[] getOutput(List<Either<Row, Row>> inputOrOutput) {
        return inputOrOutput.stream()
                .filter(Either::isRight)
                .map(Either::right)
                .toArray(Row[]::new);
    }

    @Test
    public void testFromChangelogStream() {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // create a changelog DataStream
        DataStream<Row> dataStream = env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
        Table table = tableEnv.fromChangelogStream(dataStream);

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
                .print();

        // === EXAMPLE 2 === 示例 2 显示了如何通过使用 upsert 模式将更新消息的数量减少 50% 来限制传入更改的种类以提高效率。
        // 可以通过为 toChangelogStream 定义主键和 upsert 更改日志模式来减少结果消息的数量
        dataStream = env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

        table = tableEnv.fromChangelogStream(dataStream,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert());
        tableEnv.createTemporaryView("InputTable_PkUpsert", table);
        tableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable_PkUpsert GROUP BY f0")
                .print();

    }


    @Test
    public void testToChangelogStream() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);



        // === EXAMPLE 1 ===
        // convert to DataStream in the simplest and most general way possible (no event-time)

        Table simpleTable = tableEnv
                .fromValues(
                        row("Alice", 12),
                        row("Alice", 2),
                        row("Bob", 12)
                )
                .as("name", "score")
                .groupBy($("name"))
                .select($("name"), $("score").sum());

        tableEnv
                .toChangelogStream(simpleTable)
                .executeAndCollect()
                .forEachRemaining(System.out::println);



        // === EXAMPLE 2 ===
        // convert to DataStream in the simplest and most general way possible (with event-time)
        // create Table with event-time
        tableEnv.executeSql("CREATE TABLE GeneratedTable (\n" +
                "    name STRING,\n" +
                "    score INT,\n" +
                "    event_time TIMESTAMP_LTZ(3),\n" +
                "    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND\n" +
                ") WITH (\n" +
                "'connector' = 'datagen',\n" +
                "'rows-per-second' = '1',\n" +
                "'fields.name.length' = '6',\n" +
                "'fields.score.kind' = 'sequence',\n" +
                "'fields.score.start' = '1',\n" +
                "'fields.score.end' = '10'\n" +
                ")");
        Table table = tableEnv.from("GeneratedTable");
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

        dataStream.process(new ProcessFunction<Row, Void>() {
                    @Override
                    public void processElement(Row row, Context ctx, Collector<Void> out) {
                        // prints: [name, score, event_time]
                        System.out.println(row.getKind() + " => " + row.getFieldNames(true) + "; " + row);
                        // timestamp exists twice
                        assert ctx.timestamp() == row.<Instant>getFieldAs("event_time").toEpochMilli();
                    }
                });
        env.execute();

        // === EXAMPLE 3 ===
        // convert to DataStream but write out the time attribute as a metadata column which means
        // it is not part of the physical schema anymore
        dataStream = tableEnv.toChangelogStream(table,
                Schema.newBuilder()
                        .column("name", "STRING")
                        .column("score", "INT")
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                        .build());
        // the stream record's timestamp is defined by the metadata; it is not part of the Row
        dataStream.process(new ProcessFunction<Row, Void>() {
                    @Override
                    public void processElement(Row row, Context ctx, Collector<Void> out) {
                        // prints: [name, score]
                        System.out.println(row.getKind() + " => " + row.getFieldNames(true) + "; " + row);
                        // timestamp exists once
                        System.out.println(ctx.timestamp());
                    }
                });
        // toChangelogStream(Table).executeAndCollect() 的行为等同于调用 Table.execute().collect()。
        // 但是， toChangelogStream(Table) 可能对测试更有用，因为它允许在 DataStream API 的后续 ProcessFunction 中访问生成的水印。
        CloseableIterator<Row> rowCloseableIterator = dataStream.executeAndCollect();
        List<Row> list = CollectionUtil.iteratorToList(rowCloseableIterator);
        System.out.println(list);

        // === EXAMPLE 4 ===
        // for advanced users, it is also possible to use more internal data structures for efficiency
        // note that this is only mentioned here for completeness because using internal data structures
        // adds complexity and additional type handling
        // however, converting a TIMESTAMP_LTZ column to `Long` or STRING to `byte[]` might be convenient,
        // also structured types can be represented as `Row` if needed
        dataStream = tableEnv.toChangelogStream(table,
                Schema.newBuilder()
                        .column("name", DataTypes.STRING().bridgedTo(StringData.class))
                        .column("score", DataTypes.INT())
                        .column("event_time",
                                DataTypes.TIMESTAMP_LTZ(3).bridgedTo(Long.class))
                        .build());
        dataStream.process(new ProcessFunction<Row, Void>() {
            @Override
            public void processElement(Row row, Context ctx, Collector<Void> out) {
                // prints: [name, score]
                System.out.println(row.getKind() + " => " + row.getFieldNames(true) + "; " + row);
                // timestamp exists once
                System.out.println(ctx.timestamp());
            }
        });

        dataStream.executeAndCollect().forEachRemaining(row -> {
            System.out.println(row);
        });



    }



    @Test
    public void testFromChangelogStreamUpsert() {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final List<Either<Row, Row>> inputOrOutput =
                Arrays.asList(
                        input(RowKind.INSERT, "bob", 0),
                        output(RowKind.INSERT, "bob", 0),
                        // --
                        input(RowKind.UPDATE_AFTER, "bob", 1),
                        output(RowKind.UPDATE_BEFORE, "bob", 0),
                        output(RowKind.UPDATE_AFTER, "bob", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1),
                        output(RowKind.INSERT, "alice", 1),
                        // --
                        input(RowKind.INSERT, "alice", 1), // no impact
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 2),
                        output(RowKind.UPDATE_BEFORE, "alice", 1),
                        output(RowKind.UPDATE_AFTER, "alice", 2),
                        // --
                        input(RowKind.UPDATE_AFTER, "alice", 100),
                        output(RowKind.UPDATE_BEFORE, "alice", 2),
                        output(RowKind.UPDATE_AFTER, "alice", 100));

        final DataStream<Row> changelogStream = env.fromElements(getInput(inputOrOutput));
        tableEnv.createTemporaryView(
                "t",
                tableEnv.fromChangelogStream(
                        changelogStream,
                        Schema.newBuilder().primaryKey("f0").build(),
                        ChangelogMode.upsert()));

        final Table result = tableEnv.sqlQuery("SELECT f0, SUM(f1) FROM t GROUP BY f0");
        Row[] output = getOutput(inputOrOutput);

        TableResult execute = result.execute();
        CloseableIterator<Row> collect = execute.collect();
        List<Row> list = CollectionUtil.iteratorToList(collect);
        System.out.println(output);
    }


    protected String getBootstrapServers() {
        return "192.168.51.112:9092";
    }

    @Test
    public void testChangelogDataStreamSqlWindowAgg() throws Exception {
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
                System.out.println("KafkaData: " + row);
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
        tableEnv.createTemporaryView("ods_trade_csv", table);

        tableEnv.executeSql("CREATE VIEW dws_trade_summary_10s AS \n" +
                "SELECT window_start AS win_time, group_key,\n" +
                "       count(*) cnt, sum(amount) amount_sum, PROCTIME() AS query_time,\n" +
                "       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHHmmss') , CAST(group_key AS STRING)) pk\n" +
                "FROM TABLE(TUMBLE(TABLE ods_trade_csv, DESCRIPTOR(event_time), INTERVAL '10' SECONDS))\n" +
                "GROUP BY window_start, group_key");

        Table newTable = tableEnv.sqlQuery("select * from dws_trade_summary_10s");

        DataStream<Row> rowDataStream = tableEnv.toChangelogStream(newTable);
        rowDataStream.executeAndCollect().forEachRemaining(row -> {
            System.out.println(row);
        });

    }



}
