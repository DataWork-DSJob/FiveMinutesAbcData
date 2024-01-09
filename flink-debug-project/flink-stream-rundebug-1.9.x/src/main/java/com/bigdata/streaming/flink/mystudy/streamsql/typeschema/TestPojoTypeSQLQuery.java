package com.bigdata.streaming.flink.mystudy.streamsql.typeschema;

import com.bigdata.streaming.flink.mystudy.JavaEnosTest;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Iterator;

public class TestPojoTypeSQLQuery extends JavaEnosTest {

//    static class Device{
//        private String orgId;
//        private String deviceId;
//
//
//
//        @Override
//        public String toString() {
//            String str = "{" + orgId + ',' + deviceId + '}';
//            return str;
//        }
//
//        @Override
//        public int hashCode() {
//            return super.hashCode();
//        }
//    }
//


    @Test
    public void testKeyByPojo_streamKeyBy() throws Exception {
        TypeInformation<Row> type = inputDataStream.getType();

        TypeInformation<Device> extractType = TypeExtractor.getForClass(Device.class);
        GenericTypeInfo<Device> newGenType = new GenericTypeInfo<>(Device.class);

        TableSchema schema = TableSchema.builder()
//                .field("assetId", DataTypes.RAW(new GenericTypeInfo<>(Device.class)))
                .field("assetId", DataTypes.RAW(extractType))
                .field("timeStr", DataTypes.STRING())
                .field("GenActivePW", DataTypes.INT())
                .field("APProduction", DataTypes.INT())
                .field("WindSpeed", DataTypes.DOUBLE())
                .field("ts", DataTypes.BIGINT())
                .build();

        SingleOutputStreamOperator<Row> deviceRow = inputDataStream
                .map(row -> {
                    Object assetId = row.getField(0);
                    Device device = new Device();
                    device.deviceId = assetId.toString();
                    row.setField(0, device);
                    return row;
                },schema.toRowType());

        deviceRow.keyBy(new KeySelector<Row, Device>() {
            @Override
            public Device getKey(Row value) throws Exception {
                Object field = value.getField(0);
                return (Device) field;
            }
        }).timeWindow(Time.minutes(10))
                .apply(new WindowFunction<Row, Row, Device, TimeWindow>() {
                    @Override
                    public void apply(Device device, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
                        Iterator<Row> iterator = input.iterator();
                        int count = 0;
                        while (iterator.hasNext()){
                            Row next = iterator.next();
                            count ++;
                        }
                        out.collect(Row.of(device,count));
                    }
                })
                .print();


    }


    @Test
    public void testKeyByPojo_SQLGroupBy() throws Exception {
        TypeInformation<Row> type = inputDataStream.getType();

        TypeInformation<Device> extractType = TypeExtractor.getForClass(Device.class);
        GenericTypeInfo<Device> newGenType = new GenericTypeInfo<>(Device.class);

        TableSchema schema = TableSchema.builder()
//                .field("assetId", DataTypes.RAW(new GenericTypeInfo<>(Device.class)))
                .field("assetId", DataTypes.RAW(extractType))
                .field("eventTime", DataTypes.TIMESTAMP())
                .field("GenActivePW", DataTypes.INT())
                .field("APProduction", DataTypes.INT())
                .field("WindSpeed", DataTypes.DOUBLE())
                .field("ts", DataTypes.BIGINT())
                .build();

        SingleOutputStreamOperator<Row> deviceRow = inputDataStream
                .map(row -> {
                    Object assetId = row.getField(0);
                    Device device = new Device();
                    device.deviceId = assetId.toString();
                    device.orgId = "o15630232090239";
                    row.setField(0, device);
                    Object timeStr = row.getField(1);
                    LocalDateTime eventTime = null;
                    try{
                        ZonedDateTime zonedDateTime = ZonedDateTime.parse(timeStr.toString());
                        eventTime =zonedDateTime.toLocalDateTime();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    row.setField(1, eventTime);
                    return row;
                },schema.toRowType());

        Transformation<Row> transformation = deviceRow.getTransformation();
        TypeInformation<Row> type1 = deviceRow.getType();

//        deviceRow.print("RowData: ");
//        tEnv.createTemporaryView(tableName, inputDataStream,"assetId,timeStr,GenActivePW,APProduction,WindSpeed,ts,rowtime.rowtime");

        String tableName = "tb_device";
        tEnv.createTemporaryView(tableName, deviceRow,"assetId,eventTime,GenActivePW,APProduction,WindSpeed,ts,rowtime.rowtime");
        Table table = tEnv.from(tableName);
        table.printSchema();
        tEnv.toAppendStream(table, Row.class).print("RowData: \t");

        Table queryTable = tEnv.sqlQuery("select assetId,count(*), avg(GenActivePW) from tb_device group by assetId");
        queryTable.printSchema();
        tEnv.toRetractStream(queryTable, Row.class).print("Query: \t");


    }


}
