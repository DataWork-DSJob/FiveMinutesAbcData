package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed.simpletest;

import com.bigdata.streaming.flink.mystudy.JavaBaseTest;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

public class TestMultiArgSumSQLFunc extends JavaBaseTest {

    @Test
    public void test_udfAggregateWindowSQLFunction() throws Exception {

        tEnv.registerFunction("multiSum",new MultiArgSumAggFunc());
        tEnv.registerFunction("lastValueDelta",new LastValueIncrementAggFunc());
        Table resultTable = tEnv.sqlQuery("select category, \n" +
                "   tumble_start(rowtime, interval '1' minute) as winStart, \n" +
                "   sum(num) as sum_num, \n" +
                "   count(*) as itemCount, \n" +
                "   last_value(num) as lastNum, \n" +
                "   multiSum(num,0) as multi_sum_num \n" +
                " from "+tableName+" group by category, tumble(rowtime, interval '1' minute)");
        resultTable.printSchema();
        tEnv.toRetractStream(resultTable, Row.class)
                .print();

    }


}
