package flink.debug.tableapi;

import flink.debug.FlinkSqlTableCommon;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Properties;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class FlinkTableExampleByF117 extends FlinkSqlTableCommon {

    @Test
    public void sqlBuiltInFunctionsByF117() throws Exception {
        runSqlBuiltInFunctions(null, null);
    }

    @Test
    public void sqlBuiltInOtherFunctionsByF117() throws Exception {
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());
        tableEnv.executeSql(" select 'TO_TIMESTAMP', TO_TIMESTAMP('2023-07-17T13:25:54.457798Z', 'yyyy-MM-dd''T''HH:mm:ss.SSSSSSXXXXX') ").print();

    }


    @Test
    public void simpleTableDemoByF117() throws Exception {
        runSimpleTableDemoDatagen2WindowAgg2Print(null, null);
    }

    @Test
    public void testInsertOnlyDataStreamSqlWindowAgg() throws Exception {
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
//        tableEnv.fromDataStream()
        Table table = tableEnv.fromDataStream(dataStream, schema);
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
