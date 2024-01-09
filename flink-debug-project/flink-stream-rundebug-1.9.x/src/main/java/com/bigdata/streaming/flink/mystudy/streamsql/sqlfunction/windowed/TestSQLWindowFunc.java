package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed;

import com.bigdata.streaming.flink.mystudy.JavaBaseTest;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestSQLWindowFunc extends JavaBaseTest {

    @Test
    public void test_internalSQLFunction() throws Exception {

        Table resultTable = tEnv.sqlQuery("select category, \n" +
                "   tumble_start(rowtime, interval '1' minute) as winStart, \n" +
                "   count(*) as itemCount, \n" +
                "   sum(num) as numSum, \n" +
                "   last_value(num) as lastNum, \n" +
                "   sum( (num * price)) as accountSum \n" +
                " from "+tableName+" group by category, tumble(rowtime, interval '1' minute)");
        resultTable.printSchema();
        tEnv.toRetractStream(resultTable, Row.class)
                .print();

    }

    @Test
    public void test_udfAggregateWindowSQLFunction() throws Exception {

        // accountSum(num,price) : Double/account

        Table resultTable = tEnv.sqlQuery("select category, \n" +
                "   tumble_start(rowtime, interval '1' minute) as winStart, \n" +
                "   count(*) as itemCount, \n" +
                "   sum(num) as numSum, \n" +
                "   sum( (num * price)) as accountSum \n" +
                " from "+tableName+" group by category, tumble(rowtime, interval '1' minute)");
        resultTable.printSchema();
        tEnv.toRetractStream(resultTable, Row.class)
                .print();

    }


}
