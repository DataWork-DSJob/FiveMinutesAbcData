package flink.apistudy.table;

import flink.apistudy.table.udf.HashScalarFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;

public class TestCreateRow {

    @Test
    public void testCollectDS_to_Table() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        DataStreamSource<Tuple2<String, Long>> data = env.fromElements(
                Tuple2.of("Allen", 1L),
                Tuple2.of("David", 2L),
                Tuple2.of("Lily", 3L)
        );

        data.print();
        SingleOutputStreamOperator<Object> timeDS = data.map(new MapFunction<Tuple2<String, Long>, Object>() {
            @Override
            public Object map(Tuple2<String, Long> value) throws Exception {
                return Tuple2.of(value.f0, System.currentTimeMillis());
            }
        });

//        TableEnvironment tableEnv = TableEnvironment.create();


        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        stEnv.registerDataStream("tb_data",timeDS);

        Table resultTable = stEnv.sqlQuery("select * from tb_data");
        resultTable.printSchema();
        DataStream<Row> resultDS = stEnv.toAppendStream(resultTable, Row.class);
        resultDS.print();

        env.execute(this.getClass().getSimpleName());


    }

    @Test
    public void testCreateRowDS_to_Table_succeed() throws Exception {

        // 如常用的env.addSource()也有可以指定TypeInfomation的实现，当数据类型为Row时，就必须指定TypeInfomation。
        // 其它的有些非Row类型，源码中实现了可推断出TypeInfomation，这时可以不用指定。

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000L);

        ArrayList<Row> rowData = new ArrayList<>();
        rowData.add(Row.of("Allen",5,3.21)); // 会抽取第一个Data 来生成 typeInfo: TypeInformation<OUT>
        rowData.add(Row.of("David",7,null));

        TableSchema tableSchema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .field("num", DataTypes.DOUBLE())
                .build();

        RowTypeInfo rowTypeInfo = new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
        TypeInformation<Row> rowType1 = tableSchema.toRowType();

        DataStreamSource<Row> rowDS = env.fromCollection(rowData,rowTypeInfo);
        rowDS.print();
        TypeInformation<Row> type = rowDS.getType();
        System.out.println(type);

        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env);
        String tableName = "tb_test";
        stEnv.registerDataStream(tableName,rowDS);

        Table resultTable = stEnv.sqlQuery("select * from "+tableName);
        resultTable.printSchema();
        DataStream<Row> resultDS = stEnv.toAppendStream(resultTable, Row.class);
        resultDS.print();

        stEnv.createTemporaryFunction("hashCode", HashScalarFunction.class);
        stEnv.sqlQuery("select name, hashCode(name) name_hash from " + tableName).execute().print();

        stEnv.sqlQuery("select name, age, hashCode(age) age_hash from " + tableName).execute().print();

        Thread.sleep(3000);
        env.execute(this.getClass().getSimpleName());
        Thread.sleep(5000);

    }


}
