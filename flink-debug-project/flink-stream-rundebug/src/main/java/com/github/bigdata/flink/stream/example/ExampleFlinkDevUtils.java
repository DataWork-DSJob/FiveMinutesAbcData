package com.github.bigdata.flink.stream.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.bigdata.flink.common.DebugSink;
import com.github.bigdata.flink.common.FlinkDevUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ExampleFlinkDevUtils extends FlinkDevUtils {

    @Test
    public void testKafkaSource() throws Exception {

        kafkaInputSource.map((line) -> {
            JSONObject json;
            try{
                json = JSON.parseObject(line);
                return json.toJSONString();
            }catch (Exception e){
                e.printStackTrace();
            }
            return "";
        })
                .print();


        env.execute(this.getClass().getSimpleName());

    }


    @Test
    public void testKafkaSourceToFileSink() throws Exception {

        kafkaInputSource
                .map((line) -> {
                    JSONObject json;
                    try{
                        json = JSON.parseObject(line);
                        String newLine = json.toJSONString();
                        if(System.currentTimeMillis() % 1000 ==0){
//                            System.out.println(newLine);
                            Thread.sleep(10);
                        }
                        return newLine;
                    }catch (Exception e){
//                        e.printStackTrace();
                    }
                    return "";
                })

                .addSink(StreamingFileSink
                        .forRowFormat(new Path("hdfs://bdnode102:9000/flink/output"), new SimpleStringEncoder<String>())
                        .withBucketCheckInterval(1000*5)
                        .build()
                )
        ;


        env.execute(this.getClass().getSimpleName());

    }


    @Test
    public void test() throws Exception {
        inputDataStream
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(0);
                        return category.toString();
                    }
                })
                .print()
        ;


        Table tumbleTable = tEnv.sqlQuery("select assetId, " +
                "tumble_start(rowtime, interval '1' minute) as winStart,\n" +
                " count(*) as cnt,\n" +
                " (avg(GenActivePW)/60.0) as PW_Prod_1m, \n" +
                " last_value(APProduction) - first_value(APProduction) as APP_prod_delta, \n" +
                " avg(WindSpeed) \n" +
                " from "+tableName+"\n" +
                " group by assetId, tumble(rowtime,interval '1' minute) ");
        tumbleTable.printSchema();
        tEnv.toRetractStream(tumbleTable, Row.class).print("WindowSQL Result : \t");

        DebugSink resultSink = new DebugSink();
        tEnv.toAppendStream(tumbleTable, Row.class)
                .addSink(resultSink);

        env.execute(this.getClass().getSimpleName());
        this.isExecuted = true;

        List<Row> result = resultSink.getResult();
        List<Row> expected = Arrays.asList(
                Row.of("TPerf08_deviceAst_1","2021-04-07T16:00",5,  10.0,   30,     46.35),
                Row.of("TPerf08_deviceAst_2","2021-04-07T16:00",2,  12.75,  10,     66.0),
                Row.of("TPerf08_deviceAst_1","2021-04-07T16:00",1,  null,   0,     null),
                Row.of("TPerf08_deviceAst_1","2021-04-07T16:00",1,  8.333333,   0,     null)
        );
        Assert.assertEquals(expected,result);

    }


}
