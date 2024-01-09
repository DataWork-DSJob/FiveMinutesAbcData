package flink.debug.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import flink.debug.pressure.TrashSink;
import flink.debug.utils.DebugCommMethod;
import flink.debug.utils.RowUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class BasePipeline4Test extends DebugCommMethod {

    public SingleOutputStreamOperator<String> transformString2Json2Watermark2Window(DataStream<String> kafkaDataStream) {
        SingleOutputStreamOperator<JSONObject> jsonObjDStream = kafkaDataStream
                .map(line -> {
                    JSONObject json = JSON.parseObject(line);
                    json.put("timeMillis", System.currentTimeMillis());
                    return json;
                })
                .flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                        long nanoTime = System.nanoTime();
                        value.put("Process2_flatMap", nanoTime);
                        out.collect(value);
                    }
                });

        jsonObjDStream.map(json -> {
            return json.toJSONString();
        }).addSink(new TrashSink<>());

        SingleOutputStreamOperator<String> outDS = jsonObjDStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(JSONObject element) {
                        Long timeMillis = element.getLong("timeMillis");
                        if (null == timeMillis) {
                            timeMillis = System.currentTimeMillis();
                        }
                        return timeMillis;
                    }
                })
                .map(json -> {
                    json.put("tid", Thread.currentThread().getId());
                    String key;
                    double random = Math.random();
                    if (random > 0.7) {
                        key = "Key_1";
                    } else if (random > 0.3) {
                        key = "Key_2";
                    } else {
                        key = "Key_3";
                    }
                    json.put("groupKey", key);
                    return json;
                })
                .keyBy(new MyKeySelector())
                .timeWindow(Time.seconds(3))
                .process(new MyJsonProcWindow())
                .map(k -> k.toJSONString());
        return outDS;
    }


    public void buildTradeAnalysisWindowDemo(StreamExecutionEnvironment env, int maxBatch) throws Exception {

        DataStreamSource<String> inputDs = createTradeLogSource(env, maxBatch);
        SingleOutputStreamOperator<Row> eventDs = inputDs
                .flatMap(new MyRowFlatMapFunc())
                .assignTimestampsAndWatermarks(new MyPunctuatedWatermarksAssignerAscendingTimestamp())
//                .assignTimestampsAndWatermarks(new MyPeriodicWatermarksAssignerLikeOrderless(Time.seconds(5)))
                ;
        OutputTag<Tuple2<String, Row>> lateData = new OutputTag<Tuple2<String, Row>>("latencyData",
                TypeInformation.of(new TypeHint<Tuple2<String, Row>>() {
                }));

        SingleOutputStreamOperator<Row> sumDs = eventDs
                .map(new MapFunction<Row, Tuple2<String, Row>>() {
                    @Override
                    public Tuple2<String, Row> map(Row value) throws Exception {
                        String key = RowUtils.getOrDefault(value, 1, "");
                        return Tuple2.of(key, value);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Row>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Row> value) throws Exception {
                        return value.f0;
                    }
                })
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(lateData)
                .trigger(PurgingTrigger.of(CountTrigger.of(10)))
                .trigger(ProcessingTimeTrigger.create())
                .trigger(EventTimeTrigger.create())
                .trigger(MyTriggerLikeCountEvent.create())
                .aggregate(new MyRowAggregateFunc(), new MyRowProcessFunc())
                ;

        sumDs.print("sumDs");
        sumDs.getSideOutput(lateData).map(new MapFunction<Tuple2<String, Row>, String>() {
            @Override
            public String map(Tuple2<String, Row> value) throws Exception {
                return value.f1.toString();
            }
        }).print("LateData");


    }

    private DataStreamSource<String> createTradeLogSource(StreamExecutionEnvironment env, int maxBatch) {
        String lines = "1,keyA, 1676101001, 10.0\n" +
                "2,keyA, 1676101005, 10.0\n" +
                "3,keyA, 1676101003, 10.1\n" +
                "\n" +
                "4,keyA, 1676101022, 10.0\n" +
                "5,keyA, 1676101004, 10.2\n" +
                "6,keyA, 1676101023, 10.0\n" +
                "7,keyA, 1676101021, 10.1\n" +
                "\n" +
                "8,keyA, 1676101032, 10.0\n" +
                "9,keyA, 1676101033, 10.0\n" +
                "10,keyA, 1676101034, 10.0\n" +
                "11,keyA, 1676101031, 10.1\n" +
                "12,keyA, 1676101035, 10.0\n" +
                "13,keyA, 1676101002, 10.3\n" +
                "\n" +
                "14,keyA, 1676102042, 10.0\n" +
                "15,keyA, 1676102043, 10.0\n" +
                "16,keyA, 1676101024, 10.3\n" +
                "17,keyA, 1676102044, 10.0\n" +
                "18,keyA, 1676102041, 10.1\n" +
                "19,keyA, 1676102045, 10.0\n" +
                "\n" +
                "20,keyA, 1676105051, 10.0\n" +
                "21,keyA, 1676105052, 10.0\n" +
                "22,keyA, 1676101025, 10.4\n" +
                "23,keyA, 1676105053, 10.0\n" +
                "24,keyA, 1676105054, 10.0\n" +
                "25,keyA, 1676105055, 10.0"
                ;
        String[] lineArray = lines.split("\n");
        DataStreamSource<String> strSource = env.addSource(new MyStringSource(maxBatch, lineArray));
        return strSource;
    }


}
