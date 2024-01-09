package com.bigdata.streaming.flink.mystudy;

import javafx.util.Pair;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public class JavaEnosTest {


    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;
    protected TableSchema tableSchema;
    protected DataStream<Row> inputDataStream;

    protected String tableName;
    protected Table inputTable;

    protected boolean isExecuted;

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
                .field("assetId", DataTypes.STRING())
                .field("timeStr", DataTypes.STRING())
                .field("GenActivePW", DataTypes.INT())
                .field("APProduction", DataTypes.INT())
                .field("WindSpeed", DataTypes.DOUBLE())
                .field("ts", DataTypes.BIGINT())
                .build();

        ZonedDateTime todayStart = LocalDate.now().atStartOfDay(ZoneId.systemDefault());
        ZonedDateTime afterHour = todayStart.plusHours(1);
        this.inputDataStream = env.fromElements(
                //      0                       1                                           2           3               4           5
                //      assetId                 time                                    GenActivePW  APProduction    WindSpeed      ts
                //      assetId                 time                                      有功功率     表读数          风速
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusSeconds(0))   ,   600,        1010,           36.5,       0L),
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusSeconds(5))   ,   600,        1020,           null,       null),
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusSeconds(10))  ,   600,        1030,           56.2,       null),
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusSeconds(59))  ,   600,        1050,           null,       null),
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusSeconds(15))  ,   null,       1040,           null,       null),
                // window 2,3 ?
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusMinutes(21))  ,   500,        4010,           null,       null),
//                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusMinutes(18))  ,   700,        3010,           16.3,       null),
                Row.of("TPerf08_deviceAst_1",  formatAsStr(todayStart.plusMinutes(10))  ,   null,       2010,           null,       null),


                Row.of("TPerf08_deviceAst_2",  formatAsStr(afterHour.plusSeconds(10))   ,   600,        10010,           null,       null),
                Row.of("TPerf08_deviceAst_2",  formatAsStr(afterHour.plusSeconds(20))   ,   930,        10020,           66.0,       null)

        )
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        String timeStr = (String) value.getField(1);
                        long ts = ZonedDateTime.parse(timeStr).toInstant().toEpochMilli();
                        value.setField(5, ts);
                        return value;
                    }
                },this.tableSchema.toRowType())
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(5);
                        return (long) time;
                    }
                });

        this.tableName = "tb_test";
        tEnv.createTemporaryView(tableName, inputDataStream,"assetId,timeStr,GenActivePW,APProduction,WindSpeed,ts,rowtime.rowtime");
        this.inputTable = tEnv.from(tableName);
        inputTable.printSchema();
        tEnv.toAppendStream(inputTable, Row.class).print("Before Test: \t");

        this.isExecuted=false;
    }

    private String formatAsStr(ZonedDateTime zonedDateTime) {
        return zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    @After
    public void destroy() throws Exception {
        if(!this.isExecuted){
            env.execute(this.getClass().getSimpleName());
        }
    }

    public static Pair<String, String> getDayStartEndTime(String zoneId, LocalDate localDate, DateTimeFormatter timeFormatter) {
        if(null == zoneId){
            zoneId = ZoneId.systemDefault().getId();
        }
        if(null == localDate){
            localDate = LocalDate.now();
        }
        if(null == timeFormatter){
            timeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        }
        ZonedDateTime dayStartZonedTime =localDate.atStartOfDay(ZoneId.of(zoneId));
        String zoneDayStartTimeStr = dayStartZonedTime.format(timeFormatter);
        String zoneDayEndTimeStr = dayStartZonedTime.plusDays(1).plusSeconds(-1).format(timeFormatter);
        return new Pair<>(zoneDayStartTimeStr, zoneDayEndTimeStr);
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
