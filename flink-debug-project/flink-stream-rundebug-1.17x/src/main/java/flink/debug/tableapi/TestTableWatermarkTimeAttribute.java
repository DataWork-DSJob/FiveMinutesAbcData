package flink.debug.tableapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.sourceWatermark;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestDatastreamRow2Table
 * @description: flink.debug.tableapi.dsrow2table.TestDatastreamRow2Table
 * @author: jiaqing.he
 * @date: 2023/11/26 9:24
 * @version: 1.0
 */
public class TestTableWatermarkTimeAttribute {



    /** POJO that is a generic type in DataStream API. */
    public static class ImmutablePojo {
        private final Boolean b;

        private final Double d;

        public ImmutablePojo(Double d, Boolean b) {
            this.d = d;
            this.b = b;
        }

        public Boolean getB() {
            return b;
        }

        public Double getD() {
            return d;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ImmutablePojo that = (ImmutablePojo) o;
            return Objects.equals(b, that.b) && Objects.equals(d, that.d);
        }

        @Override
        public int hashCode() {
            return Objects.hash(b, d);
        }
    }

    /** POJO that has no field order in DataStream API. */
    public static class ComplexPojo {
        public int c;

        public String a;

        public ImmutablePojo p;

        static ComplexPojo of(int c, String a, ImmutablePojo p) {
            final ComplexPojo complexPojo = new ComplexPojo();
            complexPojo.c = c;
            complexPojo.a = a;
            complexPojo.p = p;
            return complexPojo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComplexPojo that = (ComplexPojo) o;
            return c == that.c && Objects.equals(a, that.a) && Objects.equals(p, that.p);
        }

        @Override
        public int hashCode() {
            return Objects.hash(c, a, p);
        }
    }

    @Test
    public void testFromAndToDataStreamWithPojo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final ComplexPojo[] pojos = {
                ComplexPojo.of(42, "hello", new ImmutablePojo(42.0, null)),
                ComplexPojo.of(42, null, null)
        };

        final DataStream<ComplexPojo> dataStream = env.fromElements(pojos);

        // reorders columns and enriches the immutable type
        final Table table =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("c", INT())
                                .column("a", STRING())
                                .column("p", DataTypes.of(ImmutablePojo.class))
                                .build());

//        testSchema(
//                table,
//                Column.physical("c", INT()),
//                Column.physical("a", STRING()),
//                Column.physical(
//                        "p",
//                        STRUCTURED(
//                                ImmutablePojo.class, FIELD("d", DOUBLE()), FIELD("b", BOOLEAN()))));

        tableEnv.createTemporaryView("t", table);

        final TableResult result = tableEnv.executeSql("SELECT p, p.d, p.b FROM t");
        result.print();

        testResult(
                result,
                Row.of(new ImmutablePojo(42.0, null), 42.0, null),
                Row.of(null, null, null));

        testResult(tableEnv.toDataStream(table, ComplexPojo.class), pojos);
    }



    private DataStream<Tuple3<Long, Integer, String>> getWatermarkedDataStream(StreamExecutionEnvironment env) {
        final DataStream<Tuple3<Long, Integer, String>> dataStream =
                env.fromCollection(
                        Arrays.asList(
                                Tuple3.of(1L, 42, "a"),
                                Tuple3.of(2L, 5, "a"),
                                Tuple3.of(3L, 1000, "c"),
                                Tuple3.of(100L, 1000, "c")),
                        Types.TUPLE(Types.LONG, Types.INT, Types.STRING));

        return dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, Integer, String>>forMonotonousTimestamps()
                        .withTimestampAssigner((ctx) -> (element, recordTimestamp) -> element.f0));
    }

    @Test
    public void testFromDsWithWatermark3() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final DataStream<Tuple3<Long, Integer, String>> dataStream = getWatermarkedDataStream(env);

        final DataStream<Row> changelogStream = dataStream
                        .map(t -> Row.ofKind(RowKind.INSERT, t.f1, t.f2))
                        .returns(Types.ROW(Types.INT, Types.STRING));

        // derive physical columns and add a rowtime
        final Table table =
                tableEnv.fromChangelogStream(
                        changelogStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", TIMESTAMP_LTZ(3))
                                // uses Table API expressions
                                .columnByExpression("computed", $("f1").upperCase())
                                .watermark("rowtime", sourceWatermark())
                                .build());
        tableEnv.createTemporaryView("t", table);

        // access and reorder columns
        final Table reordered = tableEnv.sqlQuery("SELECT computed, rowtime, f0 FROM t");

        // write out the rowtime column with fully declared schema
        final DataStream<Row> result = tableEnv.toChangelogStream(
                        reordered,
                        Schema.newBuilder()
                                .column("f1", STRING())
                                .columnByMetadata("rowtime", TIMESTAMP_LTZ(3))
                                // uses Table API expressions
                                .columnByExpression("ignored", $("f1").upperCase())
                                .column("f0", INT())
                                .build());

        // test event time window and field access
        testResult(
                result.keyBy(k -> k.getField("f1"))
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                        .<Row>apply(
                                (key, window, input, out) -> {
                                    int sum = 0;
                                    for (Row row : input) {
                                        sum += row.<Integer>getFieldAs("f0");
                                    }
                                    out.collect(Row.of(key, sum));
                                })
                        .returns(Types.ROW(Types.STRING, Types.INT)),
                Row.of("A", 47),
                Row.of("C", 1000),
                Row.of("C", 1000));
    }



    private static void testResult(TableResult result, Row... expectedRows) {
        final List<Row> actualRows = CollectionUtil.iteratorToList(result.collect());
        assertThat(actualRows, containsInAnyOrder(expectedRows));
    }

    @SafeVarargs
    private static <T> void testResult(DataStream<T> dataStream, T... expectedResult)
            throws Exception {
        try (CloseableIterator<T> iterator = dataStream.executeAndCollect()) {
            final List<T> list = CollectionUtil.iteratorToList(iterator);
            assertThat(list, containsInAnyOrder(expectedResult));
        }
    }

    @Test
    public void testWm2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final LocalDateTime localDateTime1 = LocalDateTime.parse("1970-01-01T00:00:00.000");
        final LocalDateTime localDateTime2 = LocalDateTime.parse("1970-01-01T01:00:00.000");

        final DataStream<Tuple2<LocalDateTime, String>> dataStream =
                env.fromElements(
                        new Tuple2<>(localDateTime1, "alice"),
                        new Tuple2<>(localDateTime2, "bob"));

        final Table table =
                tableEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .column("f0", "TIMESTAMP(3)")
                                .column("f1", "STRING")
                                .watermark("f0", "SOURCE_WATERMARK()")
                                .build());

        tableEnv.createTemporaryView("t", table);

        final TableResult result = tableEnv.executeSql("SELECT f0,f1 FROM t");
        result.print();


    }

    @Test
    public void testAddRowtimeWm1_ByDsAssign_FieldNames() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                        Row.of("Alice", 12, 1702210118003L),
                        Row.of("Bob", 10, 1702210129003L),
                        Row.of("Alice", 100, 1702210143003L))
                . assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object timeFieldVal = element.getField(2);
                        return (long) timeFieldVal;
                    }
                })
                ;

        String[] fieldNames = {"name", "score"};
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream, $("name"), $("score"), $("event_time").rowtime());
        tbEnv.registerTable("input", inputTable);

        Table queryTable = tbEnv.sqlQuery("select window_start, name, count(*) cnt, sum(score) sc_sum " +
                "from TABLE(TUMBLE(TABLE input, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)) " +
                "group by window_start, name");
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        env.execute();

    }

    @Test
    public void testAddRowtimeWm2_BySchema_succeed() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[] {Types.STRING,Types.INT, Types.LOCAL_DATE_TIME},
                new String[] {"name", "score", "event_time"});
        LocalDateTime now = LocalDateTime.now();

        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                String[] names = {"Alice", "Bob", "Lily"};
                Random random = new Random();
                for (long i = 0; i < 1000; i++) {
                    ctx.collect(Row.of(
                            names[random.nextInt(3)],
                            random.nextInt(10000),
                            LocalDateTime.now()
                    ));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() { }
        }, rowTypeInfo).name("myTradeDetailStreamSrc");

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.INT())
                // DataTypes.TIMESTAMP(3) 类型必需是 TIMESTAMP(0-3), 其他类型都会报错;
                .column("event_time", DataTypes.TIMESTAMP(3).bridgedTo(LocalDateTime.class))
//                .columnByExpression("event_time", "TO_TIMESTAMP(FROM_UNIXTIME(time_sec))")
                .watermark("event_time", "event_time - INTERVAL '10' SECOND ")
//                .watermark("event_time", "SOURCE_WATERMARK()")
                .build();

        Table inputTable = streamTableEnv.fromDataStream(dataStream, schema);
        tbEnv.registerTable("input", inputTable);

        Table queryTable = tbEnv.sqlQuery("select window_start, name, count(*) cnt, sum(score) sc_sum " +
                "from TABLE(TUMBLE(TABLE input, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)) " +
                "group by window_start, name");
//        Table queryTable = tbEnv.sqlQuery("select * from input ");
        queryTable.printSchema();
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        env.execute();

    }

    @Test
    public void testAddRowtimeWm3_ByDsAssignWm_TableSchemaByColMeta_succeed() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[] {Types.STRING,Types.INT, Types.LONG},
                new String[] {"name", "score", "event_time"});

        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> ctx) throws Exception {
                String[] names = {"Alice", "Bob", "Lily"};
                Random random = new Random();
                for (long i = 0; i < 1000; i++) {
                    ctx.collect(Row.of(
                            names[random.nextInt(3)],
                            random.nextInt(10000),
                            System.currentTimeMillis()
                    ));
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() { }
        }, rowTypeInfo).name("myTradeDetailStreamSrc")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forMonotonousTimestamps()
                                .withTimestampAssigner((ctx) -> (element, recordTimestamp) -> {
                                    Object tsValue = element.getField(2);
                                    return (long) tsValue;
                                }))
        ;

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream, Schema.newBuilder()
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP(3))
//                .watermark("row_time", "row_time - INTERVAL '10' SECOND ")
                .watermark("rowtime", "SOURCE_WATERMARK()")
                .build());
        tbEnv.registerTable("input", inputTable);

        Table queryTable = tbEnv.sqlQuery("select window_start, name, count(*) cnt, sum(score) sc_sum " +
                "from TABLE(TUMBLE(TABLE input, DESCRIPTOR(rowtime), INTERVAL '10' SECONDS)) " +
                "group by window_start, name");
//        Table queryTable = tbEnv.sqlQuery("select * from input ");
        queryTable.printSchema();
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        env.execute();

    }

    @Test
    public void testAddRowtimeWm4_ByDsAssignWm_TableSchemaByCol() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[] {Types.STRING,Types.INT, Types.LOCAL_DATE_TIME},
                new String[] {"name", "score", "event_time"});

        DataStream<Row> dataStream =env.addSource(new SourceFunction<Row>() {
                    @Override
                    public void run(SourceContext<Row> ctx) throws Exception {
                        String[] names = {"Alice", "Bob", "Lily"};
                        Random random = new Random();
                        for (long i = 0; i < 1000; i++) {
                            ctx.collect(Row.of(
                                    names[random.nextInt(3)],
                                    random.nextInt(10000),
//                                    System.currentTimeMillis()
                                    LocalDateTime.now()
//                                    Timestamp.from(Instant.now())
                            ));
                            Thread.sleep(1000);
                        }
                    }
                    @Override
                    public void cancel() { }
                }, rowTypeInfo).name("myTradeDetailStreamSrc")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Row>forMonotonousTimestamps()
                                .withTimestampAssigner((ctx) -> (element, recordTimestamp) -> {
                                    LocalDateTime tsValue = (LocalDateTime) element.getField(2);
                                    return tsValue.toInstant(ZoneOffset.UTC).toEpochMilli();
                                }))

                ;

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream, Schema.newBuilder()
//                .column("name", DataTypes.STRING())
//                .column("score", DataTypes.INT())
                .columnByMetadata("event_time", DataTypes.TIMESTAMP(3))
                .watermark("event_time", "event_time - INTERVAL '10' SECOND ")
                .build());
        tbEnv.registerTable("input", inputTable);

        Table queryTable = tbEnv.sqlQuery("select window_start, name, count(*) cnt, sum(score) sc_sum " +
                "from TABLE(TUMBLE(TABLE input, DESCRIPTOR(event_time), INTERVAL '10' SECONDS)) " +
                "group by window_start, name");
//        Table queryTable = tbEnv.sqlQuery("select * from input ");
        queryTable.printSchema();
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        env.execute();

    }



}
