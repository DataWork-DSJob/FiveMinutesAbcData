package flink.debug.tableapi;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestDatastreamRow2Table
 * @description: flink.debug.tableapi.dsrow2table.TestDatastreamRow2Table
 * @author: jiaqing.he
 * @date: 2023/11/26 9:24
 * @version: 1.0
 */
public class TestTableStatement {

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

        StreamStatementSet stmtSet = tableEnv.createStatementSet();

        stmtSet.addInsertSql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%' ");
        stmtSet.addInsertSql("INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%' ");
        stmtSet.execute();

        final TableResult result = tableEnv.executeSql("SELECT f0,f1 FROM t");
        result.print();


    }

    @Test
    public void testStatementSet1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TEMPORARY TABLE Orders (\n" +
                "    product AS 'Rubber', \n" +
                "    amount INT) \n" +
                "WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '2',\n" +
                "    'fields.amount.kind' = 'sequence',\n" +
                "    'fields.amount.start' = '10',\n" +
                "    'fields.amount.end' = '10000'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH ('connector' = 'print') ");
        tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH ('connector' = 'print') ");

        StatementSet stmtSet = tEnv.createStatementSet();
        // `addInsertSql` 方法每次只接收单条 INSERT 语句
        stmtSet.addInsertSql(
                "INSERT INTO RubberOrders SELECT product, amount FROM Orders ");
        stmtSet.addInsertSql(
                "INSERT INTO GlassOrders SELECT product, amount FROM Orders ");
        // 执行刚刚添加的所有 INSERT 语句
        TableResult tableResult2 = stmtSet.execute();
        tableResult2.print();
        tableResult2.await();
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult2.getJobClient().get().getJobStatus());

    }


    @Test
    public void testStatementSetByExeSql() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TEMPORARY TABLE Orders (\n" +
                "    product AS 'Rubber', \n" +
                "    amount INT) \n" +
                "WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'rows-per-second' = '2',\n" +
                "    'fields.amount.kind' = 'sequence',\n" +
                "    'fields.amount.start' = '10',\n" +
                "    'fields.amount.end' = '10000'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH ('connector' = 'print') ");
        tEnv.executeSql("CREATE TABLE GlassOrders(product VARCHAR, amount INT) WITH ('connector' = 'print') ");


        // `addInsertSql` 方法每次只接收单条 INSERT 语句
        tEnv.executeSql(
                "INSERT INTO RubberOrders SELECT product, amount FROM Orders ");

        tEnv.executeSql(
                "INSERT INTO GlassOrders SELECT product, amount FROM Orders ").await();
        // 执行刚刚添加的所有 INSERT 语句




    }



}
