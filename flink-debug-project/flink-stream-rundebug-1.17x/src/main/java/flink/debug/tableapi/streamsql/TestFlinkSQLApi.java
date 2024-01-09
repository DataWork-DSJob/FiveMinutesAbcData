package flink.debug.tableapi.streamsql;

import flink.debug.FlinkSqlTableCommon;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.Random;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestFlinkSQLApi
 * @description: flink.debug.tableapi.streamsql.TestFlinkSQLApi
 * @author: jiaqing.he
 * @date: 2023/12/10 20:59
 * @version: 1.0
 */
public class TestFlinkSQLApi extends FlinkSqlTableCommon {

    @Test
    public void testMyStream2Paimon() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // create a changelog DataStream
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
                new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.LONG},
                new String[]{"trade_id", "client_id", "trade_amount", "td_offset"}
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
        }, rowTypeInfo).name(this.getClass().getSimpleName());
        Table table = tableEnv.fromChangelogStream(dataStream);
        tableEnv.createTemporaryView("InputTable", table);

        tableEnv.executeSql("create table print_table (trade_id String, client_id String, trade_amount Int, td_offset BIGINT) with ('connector' = 'print')");
        TableResult tableResult = tableEnv.executeSql("insert into print_table select * from InputTable");//                .await()
//        tableResult.print();
//        env.execute();
        tableResult.getJobClient().get().getJobExecutionResult().get();
//        result.getJobClient().get().getJobExecutionResult().get();

    }



}
