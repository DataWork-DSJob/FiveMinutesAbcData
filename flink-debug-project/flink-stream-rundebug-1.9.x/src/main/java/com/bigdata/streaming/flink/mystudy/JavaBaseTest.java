package com.bigdata.streaming.flink.mystudy;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JavaBaseTest {


    @Test
    public void test() throws Exception {
        eventItemDS
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(1);
                        return category.toString();
                    }
                })
                .timeWindow(Time.minutes(1))
                .aggregate(new AggregateFunction<Row, Row, Row>() {

                    @Override
                    public Row createAccumulator() {
                        return Row.of(0L,0.0);
                    }

                    @Override
                    public Row add(Row value, Row accumulator) {
                        Object field4 = value.getField(4);
                        Object field5 = value.getField(5);
                        if(null == field4 || null == field5){
                            return accumulator;
                        }
                        int num = (int)field4;
                        double price = (double)field5;
                        if(num <=0 || price <=0){
                            return accumulator;
                        }
                        double account = num * price;
                        long newCount = 1 + (long)accumulator.getField(0);
                        double newAccount = account + (double) accumulator.getField(1);
                        return Row.of(newCount,newAccount);
                    }

                    @Override
                    public Row getResult(Row accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Row merge(Row a, Row b) {
                        long newCount = (long)a.getField(0) + (long)b.getField(0);
                        double newAccount =  (double) a.getField(1) + (double) b.getField(1);
                        return Row.of(newCount,newAccount);
                    }

                })
                .print()
        ;

        Table tumbleTable = tEnv.sqlQuery("select tumble_start(rowtime, interval '1' minute) as winStart \n" +
                " from "+tableName+" group by tumble(rowtime, interval '1' minute)");
        tumbleTable.printSchema();
        tEnv.toRetractStream(tumbleTable, Row.class).print("Before Test: \t");

    }


    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected TableSchema tableSchema;
    protected SingleOutputStreamOperator<Row> eventItemDS;

    protected String tableName;
    protected Table itemTable;

    @Before
    public void setup(){
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        this.tEnv = StreamTableEnvironment.create(env,setting);

        // item,category,timeStr,ts,num,price,rowtime.rowtime
        this.tableSchema = TableSchema.builder()
                .field("item", DataTypes.STRING())
                .field("category", DataTypes.STRING())
                .field("timeStr", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("num", DataTypes.INT())
                .field("price", DataTypes.DOUBLE())
                .build();

        this.eventItemDS = env.fromElements(
                //      0              1                2                 3    4    5
                Row.of("Item001", "Electronic", "2020-11-11 10:01:00", 0L, 2, 98.01),
                Row.of("Item002", "Electronic", "2020-11-11 10:01:59", 0L, 1, 100.22),

                Row.of("Item003", "Electronic", "2020-11-11 10:02:00", 0L, 3, 102.03),

                Row.of("Item004", "Food", "2020-11-11 11:21:00", 0L, 2, 10.01),
                Row.of("Item005", "Food", "2020-11-11 11:21:31", 0L, 1, 9.03)
        )
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        return null;
                    }
                })
//                .map(new MapFunction<Row, Row>() {
//                    private ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
//
//                    @Override
//                    public Row map(Row value) throws Exception {
//                        String timeStr = (String) value.getField(2);
//                        long ts = LocalDateTime.parse(timeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(zoneOffset).toEpochMilli();
//                        value.setField(3, ts);
//                        return value;
//                    }
//                },this.tableSchema.toRowType())

                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(3);
                        return (long) time;
                    }
                });

        this.tableName = "tb_item";
//        tEnv.createTemporaryView(tableName,eventItemDS,"item,category,timeStr,ts,num,price,rowtime.rowtime");
//        this.itemTable = tEnv.from(tableName);
        itemTable.printSchema();
        tEnv.toAppendStream(itemTable, Row.class).print("Before Test: \t");


    }

    @After
    public void destroy() throws Exception {
        env.execute(this.getClass().getSimpleName());
    }



}
