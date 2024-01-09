package flink.debug.paimon;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
/**
 * @projectName: FiveMinutesAbcData
 * @className: WriteToTable
 * @description: flink.debug.paimon.WriteToTable
 * @author: jiaqing.he
 * @date: 2023/9/16 21:32
 * @version: 1.0
 */
public class WriteToTable {

    public static void main(String[] args) {
        try {
            writeTo();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeTo() throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create a changelog DataStream
        DataStream<Row> dataStream =
                env.fromElements(
                                Row.ofKind(RowKind.INSERT, "Alice", 12),
                                Row.ofKind(RowKind.INSERT, "Bob", 5),
                                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100))
                        .returns(
                                Types.ROW_NAMED(
                                        new String[] {"name", "age"},
                                        Types.STRING, Types.INT));

        // interpret the DataStream as a Table
        Schema schema = Schema.newBuilder()
                .column("name", DataTypes.STRING())
                .column("age", DataTypes.INT())
                .build();
        Table table = tableEnv.fromChangelogStream(dataStream, schema);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='file:/tmp/paimon')");
        tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("InputTable", table);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  sink_paimon_table (name string PRIMARY KEY NOT ENFORCED,age INT)");

        tableEnv.executeSql("INSERT INTO sink_paimon_table SELECT * FROM InputTable");
//
//        // insert into paimon table from your data stream table
//        tableEnv.executeSql("SELECT * FROM InputTable").print();
//        tableEnv.executeSql("DROP TABLE IF EXISTS  sink_paimon_table ");
//        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  sink_paimon_table (name string PRIMARY KEY NOT ENFORCED,age INT)");
//        tableEnv.executeSql("INSERT INTO sink_paimon_table SELECT * FROM InputTable");
////        Table resultTable = tableEnv.sqlQuery("SELECT * FROM sink_paimon_table");
////        DataStream<Row> resultDataStream = tableEnv.toChangelogStream(resultTable);
////        resultDataStream.executeAndCollect().forEachRemaining(System.out::println);
//
//
//        tableEnv.executeSql("SELECT * FROM sink_paimon_table").print();

    }

}
