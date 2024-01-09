package org.apache.paimon;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableUtils
 * @description: org.apache.paimon.FlinkTableUtils
 * @author: jiaqing.he
 * @date: 2023/12/9 11:11
 * @version: 1.0
 */
public class FlinkTableUtils {
    public static TableEnvironment sEnv;
    public static StreamExecutionEnvironment bExeEnv;
    public static TableEnvironment bEnv;

    public static String warehouse;

    @Test
    public void test() throws Exception {
        String table = createTable(
                        Arrays.asList(
                                "currency STRING",
                                "rate BIGINT",
                                "capital_currency AS UPPER(currency)"),
                        Collections.emptyList(),
                        Collections.emptyList());

        insertIntoFromTable("tmpView", table);

    }

    public static void insertIntoFromTable(String source, String sink) throws Exception {
        sEnv.executeSql(String.format("INSERT INTO `%s` SELECT * FROM `%s`;", sink, source))
                .await();
    }


    public static String createTable(
            List<String> fieldsSpec, List<String> primaryKeys, List<String> partitionKeys) {
        return createTable(fieldsSpec, primaryKeys, partitionKeys, new HashMap<>());
    }


    public static String createTable(
            List<String> fieldsSpec,
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> options) {
        // "-" is not allowed in the table name.
        String table = ("MyTable_" + UUID.randomUUID()).replace("-", "_");
        sEnv.executeSql(buildDdl(table, fieldsSpec, primaryKeys, partitionKeys, options));
        return table;
    }


    public static String buildDdl(
            String table,
            List<String> fieldsSpec,
            List<String> primaryKeys,
            List<String> partitionKeys,
            Map<String, String> options) {
        return String.format(
                "CREATE TABLE `%s`(%s %s) %s %s;",
                table,
                String.join(",", fieldsSpec),
                buildPkConstraint(primaryKeys),
                buildPartitionSpec(partitionKeys),
                buildOptionsSpec(options));
    }


    private static String buildPkConstraint(List<String> primaryKeys) {
        if (!primaryKeys.isEmpty()) {
            return String.format(",PRIMARY KEY (%s) NOT ENFORCED", String.join(",", primaryKeys));
        }
        return "";
    }

    private static String buildPartitionSpec(List<String> partitionKeys) {
        if (!partitionKeys.isEmpty()) {
            return String.format("PARTITIONED BY (%s)", String.join(",", partitionKeys));
        }
        return "";
    }

    private static String buildOptionsSpec(Map<String, String> options) {
        if (!options.isEmpty()) {
            return String.format("WITH ( %s )", optionsToString(options));
        }
        return "";
    }

    private static String buildTableOptionsSpec(Map<String, String> hints) {
        if (!hints.isEmpty()) {
            return String.format("/*+ OPTIONS ( %s ) */", optionsToString(hints));
        }
        return "";
    }

    private static String optionsToString(Map<String, String> options) {
        List<String> pairs = new ArrayList<>();
        options.forEach((k, v) -> pairs.add(String.format("'%s' = '%s'", k, v)));
        return String.join(",", pairs);
    }

}
