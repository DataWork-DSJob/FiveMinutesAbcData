package flink.debug.tableapi.dsrow2table;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.Arrays;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestDatastreamRow2Table
 * @description: flink.debug.tableapi.dsrow2table.TestDatastreamRow2Table
 * @author: jiaqing.he
 * @date: 2023/11/26 9:24
 * @version: 1.0
 */
public class TestDataStreamRow2TableF117 {

    @Test
    public void fromDataStream2TableByStreamTbEnv() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 10),
                Row.ofKind(RowKind.INSERT, "Alice", 100));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.  StreamTableEnvironment.fromDataStream(DataStream) .as(fields...)
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        tableEnv.createTemporaryView("InputTable", inputTable);
        tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name").execute().print();

        // 2. StreamTableEnvironment.fromChangelogStream(DataStream)
        Table tb_test_changelog = tableEnv.fromChangelogStream(dataStream).as("name", "score");
        tableEnv.createTemporaryView("tb_test_changelog", tb_test_changelog);
        tableEnv.sqlQuery("SELECT name, SUM(score) FROM tb_test_changelog GROUP BY name").execute().print();


    }

    @Test
    public void fromDataStream2TableByTableEnv() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 10),
                Row.ofKind(RowKind.INSERT, "Alice", 100));
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.fromValues();

    }

    @Test
    public void testDsSourceToTable3_TbEnv_registerDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TypeInformation[] fieldTypes = new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO};
        String[] fieldNames = {"name", "score"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        DataStream<Row> dataStream = env.fromCollection(Arrays.asList(
                        Row.of("Alice", 200),
                        Row.of("Bob", 500),
                        Row.of("Alice", 100)),
                rowTypeInfo);

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

//        streamTableEnv.registerDataStream("input", dataStream, "name, score");
        streamTableEnv.registerDataStream("input", dataStream);

        Table queryTable = tbEnv.sqlQuery("select name, count(*) cnt, sum(score) sc_sum from input group by name");
        streamTableEnv
                .toRetractStream(queryTable, Row.class)
                .addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                        System.out.println(value.f0 + " -> " + value.f1);
                    }
                });

        env.execute("test");

    }



    @Test
    public void t3_stream2TableByStreamTbEnv_table2StreamByTbEnv() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 10),
                Row.ofKind(RowKind.INSERT, "Alice", 100));
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        Table inputTable = streamTableEnv.fromDataStream(dataStream).as("name", "score");
        streamTableEnv.createTemporaryView("InputTable", inputTable);

        // 1. 基于 TableEnvironment.executeSql() 的api
        TableEnvironment tableEnv = streamTableEnv;
        tableEnv.executeSql("create table print_table (name String, score Int) with ('connector' = 'print')");
        tableEnv.executeSql("insert into print_table select * from InputTable").await();



    }


    @Test
    public void t4_stream2TableByTbEnv_table2StreamByTbEnv() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 10),
                Row.ofKind(RowKind.INSERT, "Alice", 100));
        TableEnvironment tbEnv = StreamTableEnvironment.create(env);

//        Table inputTable = streamTableEnv.fromDataStream(dataStream).as("name", "score");
//        tbEnv.createTemporaryView("InputTable", inputTable);


        // 1. 基于 TableEnvironment.executeSql() 的api
        TableEnvironment tableEnv = tbEnv;
        tableEnv.executeSql("create table print_table (name String, score Int) with ('connector' = 'print')");
        tableEnv.executeSql("insert into print_table select * from InputTable").await();



    }

    @Test
    public void testTableSink3_StreamTbEnv_toRetractStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> dataStream = env.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));

        String[] fieldNames = {"name", "score"};
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        TableEnvironment tbEnv = streamTableEnv;

        // 1. DataStream to Table: 第一种方法 StreamTableEnvironment.fromDataStream()
        Table inputTable = streamTableEnv.fromDataStream(dataStream).as("name", "score");
        tbEnv.registerTable("input", inputTable);


        // 2. 基于 StreamTableEnvironment.toRetractStream()
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTableEnv.toRetractStream(inputTable, Row.class);

        tuple2DataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, SinkFunction.Context context) throws Exception {
                System.out.println(value.f0 + " -> " + value.f1);
            }
        });

        Table queryTable = tbEnv.sqlQuery("select name, count(*) cnt, sum(score) sc_sum from input group by name");
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
