package org.apache.flink.table.client;

import org.junit.Test;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringJoiner;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestFlinkSqlClient
 * @description: org.apache.flink.table.client.TestFlinkSqlClient
 * @author: jiaqing.he
 * @date: 2023/12/16 15:46
 * @version: 1.0
 */
public class TestFlinkSqlClient {

    public static void main(String[] args) throws Exception {
//        String sqlFileName = "test.sql";
//        String sqlFileName = "testpaimon_trade.sql";
//        String sqlFileName = "teststarrocks_trade.sql";
//        String sqlFileName = "fsql_flinkprom.sql";
        String sqlFileName = "fsql_flinkprom_Paimon2Starrocks.sql";

        URI targetTestClassDir = Thread.currentThread().getContextClassLoader().getResource("").toURI();
        Path flinkSqlFilesPath = Paths.get(Paths.get(targetTestClassDir).getParent().getParent().toString(), "flink_sql_files");
        String sqlFile = Paths.get(flinkSqlFilesPath.toString(),  sqlFileName).toAbsolutePath().toString();
        System.out.println("将执行 Flink SQL文件: " + sqlFile);
        org.apache.flink.table.client.SqlClient.main(new StringJoiner(" ")
                .add("-f").add(sqlFile)
                .toString().split(" "));
    }

    @Test
    public void test() {
//        String metricName = "flink_taskmanager_job_task_Shuffle_Netty_Input_numBuffersInRemotePerSecond";
        String metricName = "container_1702971585932_001";
        System.out.println(metricName.length());
    }


}
