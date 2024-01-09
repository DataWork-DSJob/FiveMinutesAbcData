package flink.debug.tableapi;

import flink.debug.FlinkSqlTableCommon;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class FlinkTableExampleByF112 extends FlinkSqlTableCommon {

    @Override
    protected String getBootstrapServers() {
        return "bdnode112:9092";
    }

    @Test
    public void sqlBuiltInFunctionsByF112() throws Exception {
        runSqlBuiltInFunctions(null, null);
    }


    @Test
    public void simpleTableDemoByF112() throws Exception {
        runSimpleTableDemoDatagen2WindowAgg2Print(null, null);
    }

    @Test
    public void testMyFSql() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());

        // 5. 展示 Kafka消费的数据
        tableEnv.executeSql("SHOW FUNCTIONS")
                .print();

        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), null);

    }


    @Test
    public void testKafkaGroupByWinAggByF112() throws Exception {
        runSqlKafkaGroupByWinAgg();

    }

    @Test
    public void testKafkaTumbleStartWinAggByF112() throws Exception {
        runSqlKafkaTumbleStartWinAgg();

    }

    @Test
    public void testTradeCostByKafka() throws Exception {
        StreamExecutionEnvironment env = getStreamEnv();
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());

        String bootstrapServers = getBootstrapServers();

        // 1. 消费 Kafka 数据
        tableEnv.executeSql("DROP TABLE IF EXISTS ods_system_log");
        tableEnv.executeSql("CREATE TABLE ods_system_log (\n" +
                "  `typeName` STRING,\n" +
                "  `timestamp` STRING,\n" +
                "  `metrics` MAP<STRING, DOUBLE>,\n" +
                "  `dimensions` MAP<STRING, STRING>,\n" +
                "  `otherFields` MAP<STRING, STRING>,\n" +
                "  ts AS TO_TIMESTAMP(`timestamp`, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'),\n" +
                "  WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'system_log',\n" +
                "      'connector.startup-mode' = 'latest-offset',\n" +
                "      'connector.properties.bootstrap.servers' = '"+bootstrapServers+"',\n" +
                "      'update-mode' = 'append',\n" +
                "      'format.type' = 'json'\n" +
                ")");

        //2. 展平 清洗出明细数据
        tableEnv.executeSql("DROP VIEW IF EXISTS dwd_system_log");
        tableEnv.executeSql("CREATE VIEW dwd_system_log AS \n" +
                "SELECT\n" +
                "    `typeName`,\n" +
                "    `ts` AS `timestamp`,\n" +
                "    `dimensions`,\n" +
                "    IF(`dimensions`['system_name'] IS NOT NULL, `dimensions`['system_name'], '') AS system_name,\n" +
                "    IF(`dimensions`['hostname'] IS NOT NULL, `dimensions`['hostname'], '') AS hostname,\n" +
                "    IF(`otherFields`['bus_code'] IS NOT NULL, `otherFields`['bus_code'], '') AS bus_code,\n" +
                "    IF(`otherFields`['log_level'] IS NOT NULL, `otherFields`['log_level'], '') AS log_level,\n" +
                "    `metrics`['time_cost'] AS time_cost\n" +
                "FROM ods_system_log");

        // 3. dws
        tableEnv.executeSql("DROP TABLE IF EXISTS dws_system_log_10s");
        tableEnv.executeSql("CREATE TABLE dws_system_log_10s (\n" +
                "`metric_name` STRING,\n" +
                "`win_time` STRING,\n" +
                "`dims` MAP<STRING, STRING>,\n" +
                "`metrics` MAP<STRING, DOUBLE>\n" +
                ") WITH (\n" +
                "      'connector.type' = 'kafka',\n" +
                "      'connector.version' = 'universal',\n" +
                "      'connector.topic' = 'dws_system_log_10s',\n" +
                "      'connector.startup-mode' = 'latest-offset',\n" +
                "      'connector.properties.bootstrap.servers' = '"+bootstrapServers+"',\n" +
                "      'update-mode' = 'append',\n" +
                "      'format.type' = 'json'\n" +
                ")");

        tableEnv.executeSql("SELECT\n" +
                        "    system_name,\n" +
                        "    hostname,\n" +
                        "    bus_code,\n" +
                        "    cast(count(*) as double) as num,\n" +
                        "    cast(avg(time_cost) as double) as avgcost,\n" +
                        "    TUMBLE_START(`timestamp`, INTERVAL '10' SECOND) as window_start\n" +
                        "FROM dwd_system_log\n" +
                        "WHERE log_level like '%INFO%'\n" +
                        "GROUP BY\n" +
                        "    system_name,\n" +
                        "    hostname,\n" +
                        "    bus_code,\n" +
                        "    TUMBLE(`timestamp`, INTERVAL '10' SECOND)")
                .print();

        tableEnv.executeSql("INSERT INTO dws_system_log_10s\n" +
                "SELECT\n" +
                "    'dws_system_log_10s' AS `metric_name`,\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss.SSS+08:00') AS `win_time`,\n" +
                "    Map['system_name', system_name, 'hostname', hostname, 'bus_code', bus_code] AS `dims`,\n" +
                "  Map['num', num, 'avgcost', avgcost] AS `metrics`\n" +
                "FROM\n" +
                "(\n" +
                "  SELECT\n" +
                "    system_name,\n" +
                "    hostname,\n" +
                "    bus_code,\n" +
                "    cast(count(*) as double) as num,\n" +
                "    cast(avg(time_cost) as double) as avgcost,\n" +
                "    TUMBLE_START(`timestamp`, INTERVAL '10' SECOND) as window_start\n" +
                "  FROM dwd_system_log \n" +
                "  WHERE log_level like '%INFO%' \n" +
                "  GROUP BY\n" +
                "    system_name,\n" +
                "    hostname,\n" +
                "    bus_code,\n" +
                "    TUMBLE(`timestamp`, INTERVAL '10' SECOND)\n" +
                ")");
        // 4. 查看结果数据
        tableEnv.executeSql("select * from dws_system_log_10s")
                .print();

        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), null);

    }



}
