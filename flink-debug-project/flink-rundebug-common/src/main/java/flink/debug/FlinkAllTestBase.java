package flink.debug;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.debug.base.MyPunctuatedWatermarksAssignerAscendingTimestamp;
import flink.debug.base.MyRowAggregateFunc;
import flink.debug.base.MyRowFlatMapFunc;
import flink.debug.base.MyRowProcessFunc;
import flink.debug.base.MyTriggerLikeCountEvent;
import flink.debug.entity.TestListResultSink;
import flink.debug.entity.TradeEvent;
import flink.debug.pressure.TrashSink;
import flink.debug.utils.RowUtils;
import lombok.Data;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class FlinkAllTestBase extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.110.79:9092";


    public void setUp() {
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
    }


    @Test
    public void testReuseObject()  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ExecutionConfig config = env.getConfig();
        /**
         * enableObjectReuse : objectReuse=true时,   单并行速率可到 3w-4w
         * disableObjectReuse: objectReuse=false时,  单并行速率可到 2w 左右
         *
         */
        config.enableObjectReuse();
        pressureTest(env);
    }

    @Test
    public void testAutoOptimizeByObjReuse() throws Exception {
        OutputTag<JSONObject> outSide = new OutputTag<JSONObject>("outSide", TypeInformation.of(JSONObject.class));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        JSONObject value = new JSONObject();
        value.put("origin", value.hashCode());
        SingleOutputStreamOperator<JSONObject> process = env
                .fromElements(value)
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        out.collect(value);
                        ctx.output(outSide, value);
                    }
                })
                .name("二, Process, collect-sideOutTag")
                ;

        // 主逻辑里, 值改为 A
        process
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("num", 1);
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("main_process");
                        value.put("tags", tags);
                        value.put("tag_main_process", value.hashCode());
                        return value;
                    }
                })
                .name("三, Main Map, 主map逻辑")
                .addSink(new TrashSink<>(true))
                .name("四, Main Sink,主输出")
        ;


        // 多输出逻辑 2
        process
                .getSideOutput(outSide)
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("num", 2);
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("multi_output_2");
                        value.put("tags", tags);
                        value.put("hash_tag_multiOut_2", value.hashCode());
                        return value;
                    }
                })
                .name("3.2, Multi Map, 多输出-map算子")
                .addSink(new TrashSink<>(true))
                .name("4.2 Multi Sink, 多输出-输出算子 ")
        ;

        // 侧边逻辑里, 值改为3
        process
                .getSideOutput(outSide)
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("num", 3);
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("side_output_3");
                        value.put("tags", tags);
                        value.put("hash_tag_sideOut3", value.hashCode());
                        return value;
                    }
                })
                .name("3.3, Side Map, 测流-map算子")
                .addSink(new TrashSink<>(true))
                .name("4.3 Side Sink, 测流-输出算子 ")
        ;


        boolean hasMultiOutput = false;
        // 判断是否有多输出(OutEdge)情况, 如果某个算子下游输出2个以上, 则 hasMultiOutput = true,需禁用 对象重用(ObjectReuse);
        // 这里 clearTransformations必需填false, 否则后面执行会报错;
        StreamGraph streamGraph = env.getStreamGraph( this.getClass().getSimpleName(), false);
        Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
        for (StreamNode streamNode: streamNodes) {
            if (streamNode.getOutEdges().size() > 1) {
                hasMultiOutput = true;
            }
        }
        if (hasMultiOutput) {
            env.getConfig().disableObjectReuse();
        } else {
            // AUTO_OPTIMIZE 模式下, 默认开启 ObjectReuse 优化性能
            env.getConfig().enableObjectReuse();
        }

        env.execute(this.getClass().getSimpleName());

    }


    @Test
    public void testFsState() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000 * 20, CheckpointingMode.EXACTLY_ONCE);

        Path dirPath = getOrCreateDirFromUserDir("checkpoint-fs");
        String backendPath = dirPath.toUri().toString();
        env.setStateBackend(new FsStateBackend(backendPath));

        pressureTest(env);
    }

    // 没有写好
    @Test
    public void testRocksdbState() throws Exception {
        Thread.sleep(1000 * 60 );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000 * 20, CheckpointingMode.EXACTLY_ONCE);

        Path dirPath = getOrCreateDirFromUserDir("checkpoint-rocksdb");
        String backendPath = dirPath.toUri().toString();
//        RocksDBStateBackend backend =  new RocksDBStateBackend(backendPath);
//        backend.setNumberOfTransferThreads(3);
//        RocksDBStateBackend backend =  new RocksDBStateBackend(backendPath, true);
//        backend.setPriorityQueueStateType(RocksDBStateBackend.PriorityQueueStateType.HEAP);
        env.setStateBackend(null);

        pressureTest(env);
    }



    @Test
    public void testWatermark() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000* 5L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<TradeEvent> input = env.fromElements(
                TradeEvent.create(1, "2023-01-17 16:25:32"),
                TradeEvent.create(2, "2023-01-17 16:25:48"),
                TradeEvent.create(3, "2023-01-17 15:13:15"),
                TradeEvent.create(4, "2023-01-17 16:26:25"),
                TradeEvent.create(5, "2023-01-17 16:26:40")
        );

//        DataStreamSource<TradeEvent> input = env.addSource(new TradeEventSource(-1, TradeEvent.create(1, "2023-01-17 16:25:32"),
//                TradeEvent.create(2, "2023-01-17 16:25:48")));

        TestListResultSink listResultSink = new TestListResultSink();
        OutputTag lateData = new OutputTag<TradeEvent>("latencyData", TypeInformation.of(new TypeHint<TradeEvent>() {
        }));
        SingleOutputStreamOperator<JSONObject> process = input
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TradeEvent>(Time.seconds(5)) {
                    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    @Override
                    public long extractTimestamp(TradeEvent element) {
                        long ts;
                        try {
                            String tradeTime = element.getTradeTime();
                            ts = format.parse(tradeTime).getTime();
                        } catch (Exception e) {
                            ts = System.currentTimeMillis();
                        }
                        return ts;
                    }
                })
                .keyBy(new KeySelector<TradeEvent, String>() {
                    @Override
                    public String getKey(TradeEvent value) throws Exception {
                        return "" + value.getTradeType();
                    }
                })
                .timeWindow(Time.minutes(1))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateData)
                .process(new ProcessWindowFunction<TradeEvent, JSONObject, String, TimeWindow>() {

                    public String formatAsSimpleStr(long timeMillis) {
                        String str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timeMillis));
                        return str;
                    }

                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<TradeEvent, JSONObject, String, TimeWindow>.Context context,
                                        Iterable<TradeEvent> elements, Collector<JSONObject> out) throws Exception {
                        Iterator<TradeEvent> iterator = elements.iterator();
                        JSONObject result = new JSONObject();
                        int count = 0;
                        int sum = 0;
                        while (iterator.hasNext()) {
                            TradeEvent tradeEvent = iterator.next();
                            count++;
                            sum += tradeEvent.getTradeAmount();
                        }
                        long curWm = context.currentWatermark();
                        long curWmMs = context.currentWatermark() / 1000000;
                        long start = context.window().getStart();
//                        result.put("currentWatermark", formatAsSimpleStr(context.currentWatermark()));
                        result.put("windStart", formatAsSimpleStr(context.window().getStart()));
                        result.put("key", key);
                        result.put("count", count);
                        result.put("sum", sum);
                        out.collect(result);
                    }
                });
        process.print("process");
//        process.addSink(listResultSink);

        TestListResultSink lateDataCollect = new TestListResultSink();
        process.getSideOutput(lateData)
                .print("LateData")
//                .addSink(lateDataCollect)
        ;

        try {
            env.execute(this.getClass().getSimpleName());
            Thread.sleep(1000 * 3);
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> jsonRecords = listResultSink.getResultLines();
        JSONObject expect1 = new JSONObject(){{
            put("currentWatermark", "2023-01-17 16:26:25");
            put("windStart", "2023-01-17 16:25:00");
            put("key", "1");
            put("count", 2);
            put("sum", 200);
        }};
        JSONObject expect2 = new JSONObject(){{
            put("currentWatermark", "2023-01-17 16:26:40");
            put("windStart", "2023-01-17 16:26:00");
            put("key", "1");
            put("count", 2);
            put("sum", 200);
        }};
//        assertLinesEqual(jsonRecords, expect1, expect2);

        List lateDataLines = lateDataCollect.getResultLines();
//        assertLinesEqual(lateDataLines, TradeEvent.create(3, "2023-01-17 16:24:15"));

        jsonRecords.forEach(e -> System.out.println(e));
        System.out.println(jsonRecords);
    }

    @Data
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

    static class CountAndTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private final long maxCount;

        private final ReducingStateDescriptor<Long> stateDesc =
                new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

        public CountAndTimeTrigger(long maxCount) {
            super();
            this.maxCount = maxCount;
        }


        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
            ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
            count.add(1L);
            if (count.get() >= maxCount) {
                count.clear();
                return TriggerResult.FIRE_AND_PURGE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public boolean canMerge() {
            return false;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
            ctx.mergePartitionedState(stateDesc);
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                ctx.registerProcessingTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(stateDesc).clear();
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }

    }

    static class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private MyEventTimeTrigger() {}

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return time == window.maxTimestamp() ?
                    TriggerResult.FIRE :
                    TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window,
                            OnMergeContext ctx) {
            // only register a timer if the watermark is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the watermark is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
        }

        /**
         * Creates an event-time trigger that fires once the watermark passes the end of the window.
         *
         * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
         * trigger window evaluation with just this one element.
         */
        public static MyEventTimeTrigger create() {
            return new MyEventTimeTrigger();
        }

    }


    @Test
    public void testSocketWindowAgg() throws Exception {

        /*

1,keyA, 1676101001, 10.0
2,keyA, 1676101005, 10.0
3,keyA, 1676101003, 10.1

4,keyA, 1676101022, 10.0
5,keyA, 1676101004, 10.2
6,keyA, 1676101023, 10.0
7,keyA, 1676101021, 10.1

8,keyA, 1676101032, 10.0
9,keyA, 1676101033, 10.0
10,keyA, 1676101034, 10.0
11,keyA, 1676101031, 10.1
12,keyA, 1676101035, 10.0
13,keyA, 1676101002, 10.3

14,keyA, 1676102042, 10.0
15,keyA, 1676102043, 10.0
16,keyA, 1676101024, 10.3
17,keyA, 1676102044, 10.0
18,keyA, 1676102041, 10.1
19,keyA, 1676102045, 10.0

20,keyA, 1676105051, 10.0
21,keyA, 1676105052, 10.0
22,keyA, 1676101025, 10.4
23,keyA, 1676105053, 10.0
24,keyA, 1676105054, 10.0
25,keyA, 1676105055, 10.0

         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000* 5L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputDs = env.socketTextStream("192.168.51.124", 9099);
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



        env.execute(this.getClass().getSimpleName());


    }



    private <T>  void assertLinesEqual(List<String> actualValues, T... expectedValues) {
        if (actualValues.size() != expectedValues.length) {
            throw new AssertionError("actual.size != expected.size");
        }
        for (int i = 0; i < actualValues.size(); i++) {
            String actual = actualValues.get(i);
            String expectedValue = expectedValues[i].toString();
            if (!actual.equals(expectedValue)) {
                throw new AssertionError(String.format("LineNotEqual 第 %d行不相等; expect=%s, actual=%s", i,
                        expectedValue, actual));
            }
        }
    }

    @Test
    public void testAggregateFunc_timeWindow_mySumAggregateFunc() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        env.fromElements(
                        //      0              1                2                 3    4    5
                        Row.of("Item001",   "Electronic",  "2020-11-11 10:01:00", 0L,  1,  98.01),
                        Row.of("Item002",   "Electronic",  "2020-11-11 10:01:59", 0L,  2,  100.22),

                        Row.of("Item003",   "Electronic",  "2020-11-11 10:02:00", 0L,  1,  102.03),

                        Row.of("Item004",   "Food",        "2020-11-11 10:04:00", 0L,  2,  10.01),
                        Row.of("Item005",   "Food",        "2020-11-11 10:04:01", 0L,  1,  9.03)
                )
                .map(new MapFunction<Row, Row>() {
                    private ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(Instant.now());
                    @Override
                    public Row map(Row value) throws Exception {
                        String timeStr = (String)value.getField(2);
                        long ts = LocalDateTime.parse(timeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toInstant(zoneOffset).toEpochMilli();

                        value.setField(3,ts);
                        return value;
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(3);
                        return (long)time;
                    }
                })

                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(1);
                        return category.toString();
                    }
                })
                .timeWindow(Time.minutes(1))
                .aggregate(new AggregateFunction<Row, Double, Double>() {
                    @Override
                    public Double createAccumulator() {
                        return 0.0;
                    }

                    @Override
                    public Double add(Row value, Double accumulator) {
                        int num = (int)value.getField(4);
                        double price = (double)value.getField(5);
                        double account = num * price;
                        return  account + accumulator;
                    }

                    @Override
                    public Double getResult(Double accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Double merge(Double a, Double b) {
                        return a + b;
                    }
                })
                .print()


        ;


        env.execute(this.getClass().getSimpleName());
    }

    /**
     * 验证 job_name 如何设置, 是否可以中文
     * @throws Exception
     */
    @Test
    public void testSetJobNameAsChinese() throws Exception {

        final StreamExecutionEnvironment env = getStreamEnv();
        buildTradeAnalysisWindowDemo(env, 2);
//        String pipelineName = this.getClass().getSimpleName();
//        String pipelineName = "testPipelineName_1";
        // 1. 直接设置 Job_name
        String jobName = "testChineseName_中文名Pipe";
        env.execute(jobName);

        // 2.  如果没有设置 jobName的话, 会 getJobName() -> configuration.getString(PipelineOptions.NAME, DEFAULT_JOB_NAME);
        // 从 pipeline.name 配置 或用"Flink Streaming Job" 作为名称;
//        env.execute();

        // 3. 设置 pipeline.name 名字, 定义job_name; 设置失败;
//        Configuration configuration = new Configuration();
//        configuration.set(PipelineOptions.NAME,  "testChineseName_中文名Pipe_byPipelineOpts");
//        env.getConfig().setGlobalJobParameters(configuration);
//        env.execute();

    }



    public void testAll() {
        try {
            setUp();

            // 1.
            testReuseObject();
            testAutoOptimizeByObjReuse();
            testFsState();
            testRocksdbState();
            testSetJobNameAsChinese();

            // 1.
            testWatermark();
            testSocketWindowAgg();
            testAggregateFunc_timeWindow_mySumAggregateFunc();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
