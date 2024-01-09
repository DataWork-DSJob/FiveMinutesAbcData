package flink.busdemo.tradeagg;

import flink.busdemo.TradeDemoComm;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class TradeAggByPaimonStreamLake extends TradeDemoComm {


    @Test
    public void testCloseDataAgg() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> rowDataFromSocket = getRowDataFromSocket(env);
        // Close,tid1001,000,98.9,2023-10-04 10:07:23.768,4
        // type,trade_id,result_code,trade_amount,trade_time,offset
        Table table = tableEnv.fromChangelogStream(
                rowDataFromSocket.flatMap(new FlatMapFunction<Row, Row>() {
                    @Override
                    public void flatMap(Row value, Collector<Row> out) throws Exception {
                        if (null != value && value.getArity()==6) {
                            out.collect(value);
                        }
                    }
                }, new RowTypeInfo(
                    new TypeInformation[] {Types.STRING,Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING},
                    new String[] {"type", "trade_id", "result_code", "trade_amount", "trade_time", "offset"})
                ),
                Schema.newBuilder()
                    .column("type", DataTypes.STRING())
                    .column("trade_id", DataTypes.STRING())
                    .column("result_code", DataTypes.STRING())
                    .column("trade_amount", DataTypes.STRING())
                    .column("trade_time", DataTypes.STRING())
                    .column("offset", DataTypes.STRING())
                    .columnByExpression("event_time", "TO_TIMESTAMP(trade_time)")
                    .watermark("event_time", "event_time - INTERVAL '10' SECOND ")
                    .build()
        );


        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='"+getPaimonPath()+"')");
        tableEnv.executeSql("USE CATALOG paimon");

        // register the table under a name and perform an aggregation
        tableEnv.createTemporaryView("dwd_trade_close", table);

        /*
select trade_id, count(*) as cnt
from dwd_trade_close
group by trade_id
         */
        tableEnv.executeSql("select trade_id, count(*) as cnt \n" +
                        "from dwd_trade_close\n" +
                        "group by trade_id ")
                .print();


    }


}
