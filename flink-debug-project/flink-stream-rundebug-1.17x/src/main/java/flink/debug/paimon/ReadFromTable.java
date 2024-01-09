package flink.debug.paimon;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @projectName: FiveMinutesAbcData
 * @className: ReadFromTable
 * @description: flink.debug.paimon.ReadFromTable
 * @author: jiaqing.he
 * @date: 2023/9/16 21:33
 * @version: 1.0
 */
public class ReadFromTable {


    public static void readFrom() throws Exception {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='...')");
        tableEnv.executeSql("USE CATALOG paimon");

        // convert to DataStream
        Table table = tableEnv.sqlQuery("SELECT * FROM my_paimon_table");
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

        // use this datastream
        dataStream.executeAndCollect().forEachRemaining(System.out::println);

        // prints:
        // +I[Bob, 12]
        // +I[Alice, 12]
        // -U[Alice, 12]
        // +U[Alice, 14]
    }

}
