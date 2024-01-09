package flink.debug;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkSqlTableCommon
 * @description: flink.debug.FlinkSqlTableCommon
 * @author: jiaqing.he
 * @date: 2023/9/16 16:10
 * @version: 1.0
 */
public class FlinkSqlTableCommon extends FlinkDebugCommon {

    private String bootstrapServers = "bdnode124:9092";

    public void runSqlBuiltInFunctions(StreamExecutionEnvironment env, Configuration configuration) {
        if (null == env) {
            env = getStreamEnv();
        }
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /** Date Time Func
         *      DATE_FORMAT(time:String/Timestamp, format:String): String   时间格式化成字符串
         *              dateFormat(String dateStr, String toFormat) : String
         *
         *      TO_TIMESTAMP(time: String, Timestamp): 字符串转成 TimestampData 对象;
         *              toTimestampData(String dateStr): TimestampData
         *              toTimestampData(String dateStr, String format) : TimestampData
         *
         *      UNIX_TIMESTAMP: 字符串 转 Long
         *          unixTimestamp(String dateStr, String format): long
         *          unixTimestamp(String dateStr): long
         *          unixTimestamp(String dateStr, String format, TimeZone tz): long
         *
         *     FROM_UNIXTIME : Unix_ms Long -> 字符串
         *          fromUnixtime(long unixtime): String
         *          fromUnixtime(long unixtime, TimeZone tz): String
         *
         */
        // select DATE_FORMAT('2022-03-32 13:00:32', 'yyyy-MM-dd HH:mm:ss')
        tableEnv.executeSql(" select  'DATE_FORMAT', DATE_FORMAT('2022-03-25 13:00:32', 'yyyyMMdd mm:ss') ").print();
        // select TO_TIMESTAMP('2022-03-25 13:00:32')
        tableEnv.executeSql(" select 'TO_TIMESTAMP', TO_TIMESTAMP('2022-03-25 13:00:32') ").print();
        tableEnv.executeSql(" select 'UNIX_TIMESTAMP', UNIX_TIMESTAMP('2022-03-25 13:00:32') ").print();
        tableEnv.executeSql(" select 'FROM_UNIXTIME', FROM_UNIXTIME(1648184432) ").print();
        // long -> Timestamp
        tableEnv.executeSql(" select 'Long_2_Timestamp', TO_TIMESTAMP(FROM_UNIXTIME(1648184432)) ").print();
        // SELECT id, gkey, TO_TIMESTAMP(FROM_UNIXTIME(ts)) AS ts  FROM (VALUES (1, 1676101001, 'keyA'), (2, 1676101022, 'keyA'), (3, 1676101004, 'keyA')) T(id, ts, gkey)
        tableEnv.executeSql(" SELECT a, UNIX_TIMESTAMP(b) as ts FROM (VALUES (3, '2015-07-24 10:00:00')) T(a, b) ").print();
        tableEnv.executeSql(" SELECT id, gkey, TO_TIMESTAMP(FROM_UNIXTIME(ts)) AS ts  FROM (VALUES (1, 1676101001, 'keyA'), (2, 1676101022, 'keyA'), (3, 1676101004, 'keyA')) T(id, ts, gkey) ").print();

        /** Window Funcs
         *
         */

        // SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name
        tableEnv.executeSql(" SELECT name, COUNT(*) AS cnt FROM (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name) GROUP BY name ")
                .print();
        /*
        SELECT gkey, event_time, DATE_FORMAT(event_time, 'HH:mm'),  count(*) cnt
        FROM ( SELECT id, gkey, TO_TIMESTAMP(FROM_UNIXTIME(ts)) AS event_time FROM (VALUES (1, 1676101001, 'keyA'), (2, 1676101022, 'keyA'), (3, 1676101004, 'keyA')) T(id, ts, gkey) ) a
        GROUP BY gkey , event_time ;
         */
        tableEnv.executeSql("SELECT gkey, event_time, DATE_FORMAT(event_time, 'HH:mm'),  count(*) cnt \n" +
                "FROM ( SELECT id, gkey, TO_TIMESTAMP(FROM_UNIXTIME(ts)) AS event_time FROM (VALUES (1, 1676101001, 'keyA'), (2, 1676101022, 'keyA'), (3, 1676101004, 'keyA')) T(id, ts, gkey) ) a\n" +
                "GROUP BY gkey , event_time").print();

//        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), configuration);

    }

    protected String getBootstrapServers() {
        return bootstrapServers;
    }



    public void runSqlKafkaGroupByWinAgg() throws Exception {
        StreamExecutionEnvironment sEnv = getStreamEnv();
        sEnv.enableCheckpointing(1000 * 5);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);

        // Catalog
        tableEnv.executeSql("CREATE CATALOG test_mem_catalog WITH ('type'='generic_in_memory')");
        tableEnv.executeSql("USE CATALOG test_mem_catalog");

        String bootstrapServers = getBootstrapServers();

        // 1. 消费Kafka Odw数据
        tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS ods_trade_csv");
        tableEnv.executeSql("CREATE TEMPORARY TABLE ods_trade_csv (\n" +
                "    record_id BIGINT,\n" +
                "    group_key STRING,\n" +
                "    time_sec BIGINT,\n" +
                "    amount DOUBLE, \n" +
                "    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec)), \n" +
                "    WATERMARK FOR event_time AS event_time\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'ods_trade_csv',\n" +
                "'properties.bootstrap.servers' = '"+bootstrapServers+"',\n" +
                "'properties.group.id' = 'testGroupId_winIdea',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");

        // 2. 直接测试 Tumble 函数
        tableEnv.executeSql("SELECT group_key, event_time, DATE_FORMAT(event_time, 'HH:mm:ss') as tim_millis, count(*) AS cnt, PROCTIME() AS query_time \n" +
                "FROM ods_trade_csv \n" +
                "GROUP BY group_key, event_time ").print();


    }

    /**
     1,keyA, 1676101001, 10.0
     2,keyA, 1676101022, 10.0
     3,keyA, 1676101035, 10.0
     4,keyA, 1676101005, 10.0

     * @throws Exception
     */

    public void runSqlKafkaTumbleStartWinAgg() throws Exception {
        StreamExecutionEnvironment sEnv = getStreamEnv();
        sEnv.enableCheckpointing(1000 * 5);
        TableEnvironment tableEnv = StreamTableEnvironment.create(sEnv);

        // Catalog
        tableEnv.executeSql("CREATE CATALOG test_mem_catalog WITH ('type'='generic_in_memory')");
        tableEnv.executeSql("USE CATALOG test_mem_catalog");
        String bootstrapServers = getBootstrapServers();
        // 1. 消费Kafka Odw数据
        tableEnv.executeSql("DROP TEMPORARY TABLE IF EXISTS ods_trade_csv");
        tableEnv.executeSql("CREATE TEMPORARY TABLE ods_trade_csv (\n" +
                "    record_id BIGINT,\n" +
                "    group_key STRING,\n" +
                "    time_sec BIGINT,\n" +
                "    amount DOUBLE, \n" +
                "    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(time_sec)), \n" +
                "    WATERMARK FOR event_time AS event_time\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'ods_trade_csv',\n" +
                "'properties.bootstrap.servers' = '"+bootstrapServers+"',\n" +
                "'properties.group.id' = 'testGroupId_winIdea',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");

        // 2. 直接测试 Tumble 函数
        tableEnv.executeSql("SELECT\n" +
                "    group_key, event_time, DATE_FORMAT(event_time, 'HH:mm:ss') as tim_millis, \n" +
                "    TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start,\n" +
                "    count(*) AS cnt, sum(amount) AS amount_sum,\n" +
                "    PROCTIME() AS query_time \n" +
                "FROM ods_trade_csv \n" +
                "GROUP BY group_key, event_time, TUMBLE(event_time, INTERVAL '10' SECOND)\n" +
                "").print();

        tableEnv.executeSql("SELECT\n" +
                "    group_key,  TUMBLE_START(event_time, INTERVAL '10' SECOND) AS window_start,\n" +
                "    count(*) AS cnt, sum(amount) AS amount_sum,\n" +
                "    PROCTIME() AS query_time \n" +
                "FROM ods_trade_csv \n" +
                "GROUP BY group_key, TUMBLE(event_time, INTERVAL '10' SECOND)").print();


    }




}
