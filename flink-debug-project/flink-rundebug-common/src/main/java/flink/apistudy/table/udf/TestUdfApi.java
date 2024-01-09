package flink.apistudy.table.udf;

import flink.apistudy.table.udf.percentv2.MultiPercentileAggFunc;
import flink.apistudy.table.udf.percentv2.Percentile95AggFunc;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TestUdfApi {

    @Test
    public void testUdf() throws Exception {

        // 如常用的env.addSource()也有可以指定TypeInfomation的实现，当数据类型为Row时，就必须指定TypeInfomation。
        // 其它的有些非Row类型，源码中实现了可推断出TypeInfomation，这时可以不用指定。

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("city", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("num", DataTypes.DOUBLE())
                .field("hobby", DataTypes.ARRAY(DataTypes.STRING()))
                .build();
        ArrayList<Row> rowData = new ArrayList<>();
        rowData.add(Row.of("Allen He", "SH", 32, 3.21, new String[]{"Footbool", "Basketbool"})); // 会抽取第一个Data 来生成 typeInfo: TypeInformation<OUT>
        rowData.add(Row.of("David Li", "SH", 23, null, null));
        rowData.add(Row.of("Lily He", "SH", 42, 302.3, null));

        RowTypeInfo rowTypeInfo = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
        TypeInformation<Row> rowType1 = tableSchema.toRowType();

        DataStreamSource<Row> rowDS = env.fromCollection(rowData,rowTypeInfo);
        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.registerDataStream(tableName,rowDS);

        // 1. Scalar Func
        Table resultTable = stEnv.sqlQuery("select * from "+tableName);
        resultTable.printSchema();
        DataStream<Row> resultDS = stEnv.toAppendStream(resultTable, Row.class);
        resultDS.print();

        // 2. Udf Table Func, TableApi
        stEnv.from(tableName)
                .joinLateral(call(SplitTableFunction.class,$("name")))
                .select($("name"), $("age"), $("num"),$("index"), $("onRight"), $("word"), $("length"))
                .execute()
                .print();
        ;

        // 2. Udf Table Func, in SQL
        stEnv.from(tableName)
                .joinLateral(call(SplitTableFunction.class,$("name")))
                .select($("name"), $("age"), $("num"),$("index"), $("onRight"), $("word"), $("length"))
                .execute()
                .print();
        ;

        // 3. SQL api, 注册函数, Udf-Table-Func
        stEnv.createTemporaryFunction("split_table", SplitTableFunction.class);

        stEnv.sqlQuery(
                "SELECT name, age, index, word, length " +
                "FROM " + tableName + " , LATERAL TABLE (split_table(name))")
                .execute().print();
        ;

        stEnv.sqlQuery(
                "SELECT name, age, index, word, length \n" +
                        "FROM " + tableName + " LEFT JOIN LATERAL TABLE(split_table(name)) ON TRUE")
                .execute().print();
        ;

        // 4. SQL, Aggregate Udf
        stEnv.createTemporaryFunction("myAvg", MyAvgAggregateFunc.class);

        stEnv.sqlQuery(
                        "SELECT city, myAvg(age) as avg_age \n" +
                                "FROM " + tableName + " GROUP BY city")
                .execute().print();


        // 5. Table Api (没有Sql api ?)
        // 目前 SQL 中没有直接使用表聚合函数的方式，所以需要使用 Table API 的方式来调用
        stEnv.registerFunction("myTop2", new Top3TableAggFunc());
        resultTable
                .groupBy("city")
                .flatAggregate("myTop2(age) as (rank, age, percent) ")
                .select("city, rank, age, percent")
                .execute()
                .print();
        ;

        Thread.sleep(1000);

        env.execute(this.getClass().getSimpleName());


    }

    @Test
    public void dev4TradeLogLakeHouse() throws Exception {

        // 如常用的env.addSource()也有可以指定TypeInfomation的实现，当数据类型为Row时，就必须指定TypeInfomation。
        // 其它的有些非Row类型，源码中实现了可推断出TypeInfomation，这时可以不用指定。

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        TableSchema tableSchema = TableSchema.builder()
                .field("log", DataTypes.STRING())
                .field("num", DataTypes.INT())
                .field("hobby", DataTypes.ARRAY(DataTypes.STRING()))
                .build();
        String log = "2022-05-17T04:09:02.487660Z [Note] TransformInfo:20230201132710||201000042022039349092393||EXZCash||BR2580019||10000.00||27||0000||192.168.110.152||xyz";
        ArrayList<Row> rowData = new ArrayList<>();
        rowData.add(Row.of(log, 32, new String[]{"Footbool", "Basketbool"})); // 会抽取第一个Data 来生成 typeInfo: TypeInformation<OUT>
        rowData.add(Row.of(log, 42, null));
        RowTypeInfo rowTypeInfo = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
        DataStreamSource<Row> inputRowDS = env.fromCollection(rowData,rowTypeInfo);
        System.out.println(inputRowDS.getType());

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "ods_trade_log";
        stEnv.registerDataStream(tableName,inputRowDS);

        Table resultTable = stEnv.sqlQuery("select * from "+tableName);
        resultTable.printSchema();

        // 1. REGEXP_EXTRACT 正则提取
        stEnv.sqlQuery(
                        "SELECT REGEXP_EXTRACT(log, '^(.*)Z \\[\\w+\\] TransformInfo:(.*)$', 2) as logInfo, log " +
                                "FROM " + tableName )
                .execute().print();
        ;

        // 2. ScalarFunc: Split As String[], 切分成 String[] 数组
        stEnv.createTemporaryFunction("splitAsArray", SplitAsArrayFunction.class);
        stEnv.sqlQuery(
                        "SELECT splitAsArray(REGEXP_EXTRACT(log, '^(.*)Z \\[\\w+\\] TransformInfo:(.*)$', 2), '\\|\\|') as tradeInfos from ods_trade_log ")
                .execute().print();


        // 3. 从Array[String] 中提取
        stEnv.sqlQuery(
                        "SELECT \n" +
                                "  tdArr[1] as transTime,\n" +
                                "  tdArr[2] as transId,\n" +
                                "  tdArr[3] as transType,\n" +
                                "  tdArr[4] as paymentCode,\n" +
                                "  tdArr[5] as transAmount,\n" +
                                "  tdArr[6] as usedTime,\n" +
                                "  tdArr[7] as resultCode,\n" +
                                "  tdArr[8] as ip\n" +
                                "FROM (\n" +
                                "  SELECT splitAsArray(REGEXP_EXTRACT(log, '^(.*)Z \\[\\w+\\] TransformInfo:(.*)$', 2), '\\|\\|') as tdArr from ods_trade_log\n" +
                                ") ")
                .execute().print();


        Thread.sleep(1000);

//        env.execute(this.getClass().getSimpleName());


    }

    @Test
    public void testUdfPercent95() throws Exception {

        // 如常用的env.addSource()也有可以指定TypeInfomation的实现，当数据类型为Row时，就必须指定TypeInfomation。
        // 其它的有些非Row类型，源码中实现了可推断出TypeInfomation，这时可以不用指定。

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("city", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("num", DataTypes.DOUBLE())
                .field("hobby", DataTypes.ARRAY(DataTypes.STRING()))
                .build();
        ArrayList<Row> rowData = new ArrayList<>();
        rowData.add(Row.of("Allen He", "SH", 32, 1.12, new String[]{"Footbool", "Basketbool"})); // 会抽取第一个Data 来生成 typeInfo: TypeInformation<OUT>
        rowData.add(Row.of("David Li", "SH", 23, null, null));
        rowData.add(Row.of("Lily He", "SH", 42, 33.14, null));
        rowData.add(Row.of("Lily He", "SH", 42, 44.31234, null));
        rowData.add(Row.of("Lily He", "SH", 42, 55.002, null));

        RowTypeInfo rowTypeInfo = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());

        DataStreamSource<Row> rowDS = env.fromCollection(rowData,rowTypeInfo);
        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.registerDataStream(tableName,rowDS);

        // 4. SQL, Aggregate Udf
        stEnv.createTemporaryFunction("percentile95", Percentile95AggFunc.class);
        stEnv.sqlQuery(
                        "SELECT city, count(*) as cnt, percentile95(num) as ret_percentile95 \n" +
                                "FROM " + tableName + " GROUP BY city")
                .execute().print();
        Thread.sleep(1000 * 2);

        // 4. SQL, Aggregate Udf
        stEnv.createTemporaryFunction("multi_percentile", MultiPercentileAggFunc.class);
        stEnv.sqlQuery(
                        "SELECT city, count(*) as cnt, multi_percentile(num) as mulit_p_arr \n" +
                                "FROM " + tableName + " GROUP BY city")
                .execute().print();
        Thread.sleep(1000 * 10);



        env.execute(this.getClass().getSimpleName());
        Thread.sleep(1000 * 20);

    }


}
