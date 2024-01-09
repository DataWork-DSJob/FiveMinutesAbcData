package flink.debug.funcdemo;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.Date;

public class F115TestFlinkFunctions extends FlinkFunctionsDemo {


    @Test
    public void testWatermarkByF115() throws Exception {
        testWatermark();
    }

//    @Data
    class WaterSensor {
        String id;
        long ts;
        Double vc;
        String originData;
        public WaterSensor(String id, long ts, Double vc, String originData) {
            this.id = id;
            this.ts = ts;
            this.vc = vc;
            this.originData = originData;
        }
    }

    @Test
    public void testSocketWindowAggByF115() throws Exception {

        /**

         ws_001, 1609314670, 45.0
         ws_002, 1609314671, 33.0
         ws_003, 1609314672, 32.0
         ws_002, 1609314673, 23.0
         ws_003, 1609314674, 31.0
         ws_002, 1609314675, 45.0
         ws_003, 1609314676, 18.0
         ws_002, 1609314677, 34.0
         ws_003, 1609314678, 47.0
         ws_001, 1609314679, 55.0
         ws_001, 1609314680, 25.0
         ws_001, 1609314681, 25.0
         ws_001, 1609314682, 25.0
         ws_001, 1609314683, 26.0
         ws_001, 1609314684, 21.0
         ws_001, 1609314685, 24.0
         ws_001, 1609314686, 15.0
         ws_001, 1609314687, 15.0

         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000* 5L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputDs = env.socketTextStream("192.168.51.124", 9099);
        SingleOutputStreamOperator<FlinkFunctionsDemo.WaterSensor> eventDs = inputDs
                .map(new MapFunction<String, FlinkFunctionsDemo.WaterSensor>() {
                    @Override
                    public FlinkFunctionsDemo.WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        long timeSec = Long.parseLong(split[1].trim());
                        String timeStr = new Date(timeSec * 1000).toLocaleString();
                        return new FlinkFunctionsDemo.WaterSensor(split[0], timeSec * 1000, Double.parseDouble(split[2].trim()), value);
                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<FlinkFunctionsDemo.WaterSensor>(Time.seconds(2)) {
                            @Override
                            public long extractTimestamp(FlinkFunctionsDemo.WaterSensor element) {
                                return element.ts;
                            }
                        })
                ;
        OutputTag<Tuple2<String, FlinkFunctionsDemo.WaterSensor>> lateData = new OutputTag<Tuple2<String, FlinkFunctionsDemo.WaterSensor>>("latencyData", TypeInformation.of(new TypeHint<Tuple2<String, FlinkFunctionsDemo.WaterSensor>>() {
        }));

//        inputDs.map(e -> new Tuple2(e.getBytes()))
        SingleOutputStreamOperator<Tuple2<String, FlinkFunctionsDemo.WaterSensor>> sumDs = eventDs
                .map(new MapFunction<FlinkFunctionsDemo.WaterSensor, Tuple2<String, FlinkFunctionsDemo.WaterSensor>>() {
                    @Override
                    public Tuple2<String, FlinkFunctionsDemo.WaterSensor> map(FlinkFunctionsDemo.WaterSensor value) throws Exception {
                        return Tuple2.of(value.id, value);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, FlinkFunctionsDemo.WaterSensor>, String>() {
                    @Override
                    public String getKey(Tuple2<String, FlinkFunctionsDemo.WaterSensor> value) throws Exception {
                        return value.f0;
                    }
                })
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.seconds(4))
                .sideOutputLateData(lateData)
                .reduce(new ReduceFunction<Tuple2<String, FlinkFunctionsDemo.WaterSensor>>() {
                    @Override
                    public Tuple2<String, FlinkFunctionsDemo.WaterSensor> reduce(Tuple2<String, FlinkFunctionsDemo.WaterSensor> value1, Tuple2<String, FlinkFunctionsDemo.WaterSensor> value2) throws Exception {
                        double newVc = value1.f1.vc + value2.f1.vc;
                        value1.f1.vc = newVc;
                        return new Tuple2<String, FlinkFunctionsDemo.WaterSensor>(value1.f0, value1.f1);
                    }
                });

//        inputDs.print("Input");
        sumDs.print("sumDs");
        sumDs.getSideOutput(lateData).map(new MapFunction<Tuple2<String, FlinkFunctionsDemo.WaterSensor>, String>() {
            @Override
            public String map(Tuple2<String, FlinkFunctionsDemo.WaterSensor> value) throws Exception {
                return value.f1.toString();
            }
        }).print("LateData");

        env.execute(this.getClass().getSimpleName());


    }




}
