package flink.debug.funcdemo;


import com.alibaba.fastjson.JSONObject;
import flink.debug.FlinkDebugCommon;
import flink.debug.entity.TestListResultSink;
import flink.debug.entity.TradeEvent;
import flink.debug.utils.RowUtils;
import flink.debug.utils.TimeHelper;
import lombok.Data;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class FlinkFunctionsDemo extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.110.79:9092";


    @Before
    public void setUp() {
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
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


    class MyRowFlatMapFunc implements FlatMapFunction<String, Row> {
        @Override
        public void flatMap(String value, Collector<Row> out) throws Exception {
            // 25,keyA, 1676105055, 10.0
            //  0  1         2       3
            String[] split = value.split(",");
            Row row = null;
            if (null != split && split.length>3) {
                String id = split[0];
                String key = split[1];
                long timeSec = Long.parseLong(split[2].trim());
                double amount = Double.parseDouble(split[3].trim());
                String timeStr = new Date(timeSec * 1000).toLocaleString();
                // Row.of(id  0, key  1, timeSec*1000  2, amount  3, value  4);
                row = Row.of(id, key, timeSec * 1000, amount, value);
                out.collect(row);
            }
        }
    }

    /** 周期性产生的水位,
     *  generate watermarks in a periodical interval. At most every i milliseconds (configured via ExecutionConfig.getAutoWatermarkInterval()), the system will call the getCurrentWatermark() method to probe for the next watermark value
     *   周期频率: 200ms,
     *      periodic-watermarks-interval, ExecutionConfig.setAutoWatermarkInterval(), 默认 200L 毫秒;
     *      能过于频繁。因为Watermark对象是会全部流向下游的，也会实打实地占用内存，水印过多会造成系统性能下降。
     *
     */
    class MyPeriodicWatermarksAssignerLikeOrderless implements AssignerWithPeriodicWatermarks<Row> {
        private static final long serialVersionUID = 1L;
        /** The current maximum timestamp seen so far. */
        private long currentMaxTimestamp;
        /** The timestamp of the last emitted watermark. */
        private long lastEmittedWatermark = Long.MIN_VALUE;
        /**
         * The (fixed) interval between the maximum seen timestamp seen in the records
         * and that of the watermark to be emitted.
         */
        private final long maxOutOfOrderness;

        public MyPeriodicWatermarksAssignerLikeOrderless(Time maxOutOfOrderness) {
            if (maxOutOfOrderness.toMilliseconds() < 0) {
                throw new RuntimeException("Tried to set the maximum allowed " + "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
            }
            this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
            this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
        }

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            // this guarantees that the watermark never goes backwards.
            long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
            if (potentialWM >= lastEmittedWatermark) {
                lastEmittedWatermark = potentialWM;
            }
            return new Watermark(lastEmittedWatermark);
        }

        @Override
        public long extractTimestamp(Row element, long previousElementTimestamp) {
            Long timestamp = RowUtils.getOrDefault(element, 2, System.currentTimeMillis());
            if (timestamp > currentMaxTimestamp) {
                currentMaxTimestamp = timestamp;
            }
            return timestamp;
        }
    }


    /**
     *  需要依赖于事件本身的某些属性决定是否发射水印的情况。我们实现checkAndGetNextWatermark()方法来产生水印，产生的时机完全由用户控制。
     *
     */
    class MyPunctuatedWatermarksAssignerAscendingTimestamp implements AssignerWithPunctuatedWatermarks<Row> {
        private final Logger LOG = LoggerFactory.getLogger(MyPunctuatedWatermarksAssignerAscendingTimestamp.class);
        private long lastEmittedWatermark = Long.MIN_VALUE;

        @Override
        public long extractTimestamp(Row element, long previousElementTimestamp) {
            Long timestamp = RowUtils.getOrDefault(element, 2, 0L);
            if (timestamp < lastEmittedWatermark) {
                String lastWm = TimeHelper.Format.formatAsHHmmssSSS(lastEmittedWatermark);
                String eventTime = TimeHelper.Format.formatAsHHmmssSSS(timestamp);
                String previousTime = TimeHelper.Format.formatAsHHmmssSSS(previousElementTimestamp);
                long deltaMs = lastEmittedWatermark - timestamp;
                LOG.info("LateData延迟数据 ( {} < 当前水位{}, 延迟{} )  event:{}, previous:{}, wm:{} ; CurElement={}", timestamp, lastEmittedWatermark, deltaMs,
                        eventTime, previousTime, lastWm, element);
            }
            return timestamp;
        }
        @Override
        public Watermark checkAndGetNextWatermark(Row lastElement, long extractedTimestamp) {
            Long lastEventTime = RowUtils.getOrDefault(lastElement, 2, 0L);

            // 时间戳没增加 100毫秒, 更新一次水位;
            final long previousWm = this.lastEmittedWatermark;
            boolean needUpdateWm = extractedTimestamp > previousWm + 100;
            if (needUpdateWm) {
                long deltaMs = extractedTimestamp - previousWm;
                String lastWm = TimeHelper.Format.formatAsHHmmssSSS(previousWm);
                String newWm = TimeHelper.Format.formatAsHHmmssSSS(extractedTimestamp);
                LOG.debug("更新水位( {} -> {} 递增{} ms) 时间从{} 涨到{}; lastElement数据 {}", previousWm, extractedTimestamp, deltaMs, lastWm, newWm, lastElement);
                this.lastEmittedWatermark = extractedTimestamp;
            }
            return needUpdateWm ? new Watermark(extractedTimestamp) : null;
        }
    }


    static class MyTriggerLikeCountEvent extends Trigger<Object, TimeWindow> {
        // Called for every element that gets added to a pane. 处理每个元素时;
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
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            boolean isOutputTime = time == window.maxTimestamp();
            return isOutputTime ? TriggerResult.FIRE : TriggerResult.CONTINUE;
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
        public void onMerge(TimeWindow window, OnMergeContext ctx) {
            // only register a timer if the watermark is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the watermark is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
                ctx.registerEventTimeTimer(windowMaxTimestamp);
            }
        }

         static MyTriggerLikeCountEvent create() {
            return new MyTriggerLikeCountEvent();
        }

    }



    class MyRowAggregateFunc implements AggregateFunction<Tuple2<String, Row>, Row, Row> {
        private final Logger LOG = LoggerFactory.getLogger(MyRowAggregateFunc.class);
        private final int rowSize = 10;
        @Override
        public Row createAccumulator() {
            return new Row(rowSize);
        }

        @Override
        public Row add(Tuple2<String, Row> value, Row accumulator) {
            Long count = RowUtils.getOrDefault(accumulator, 0, 0L);
            accumulator.setField(0, ++count);
            // Row.of(id  0, key  1, timeSec*1000  2, amount  3, value  4);
            double sum = RowUtils.getOrDefault(accumulator, 1, 0D)
                    + RowUtils.getOrDefault( value.f1, 3, 0D);
            accumulator.setField(1, sum);

            List<String> originDataArray = RowUtils.getOrDefault(accumulator, 2, new LinkedList<String>());
            originDataArray.add(RowUtils.getOrDefault(value.f1, 4, "0L"));
            accumulator.setField(2, originDataArray);
            // accumulator: Row [ 0 count, 1 sum, 2 originDataArray ]

            String dataStr = value.f1.getField(4).toString();
            LOG.debug("WinAdd 窗口添加({}), key={}, row={}, accumulator={} ", dataStr, value.f0, value.f1, accumulator);
            return accumulator;
        }

        @Override
        public Row getResult(Row accumulator) {
            return accumulator;
        }

        @Override
        public Row merge(Row a, Row b) {
            long count = RowUtils.getOrDefault(a, 0, 0L) + RowUtils.getOrDefault(a, 0, 0L);
            a.setField(0, count);

            double sum = RowUtils.getOrDefault(a, 1, 0D) + RowUtils.getOrDefault(a, 1, 0D);
            a.setField(1, sum);

            List<String> aList = RowUtils.getOrDefault(a, 2, Collections.emptyList());
            List<String> bList = RowUtils.getOrDefault(b, 2, Collections.emptyList());
            List<String> originDataArray = new ArrayList<>(aList.size() + bList.size());
            originDataArray.addAll(aList);
            originDataArray.addAll(bList);
            a.setField(2, originDataArray);
            return a;
        }
    }

    class MyRowProcessFunc extends ProcessWindowFunction<Row, Row, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<Row, Row, String, TimeWindow>.Context context,
                            Iterable<Row> elements, Collector<Row> out) throws Exception {

            long count = 0L;
            double sum = 0D;
            List<String> originDataArray = new LinkedList<>();
            Iterator<Row> it = elements.iterator();
            while (it.hasNext()) {
                Row acc = it.next();
                count += RowUtils.getOrDefault(acc, 0, 0L);
                sum += RowUtils.getOrDefault(acc, 1, 0D);
                List<String> arr = RowUtils.getOrDefault(acc, 2, Collections.emptyList());
                if (null != acc) {
                    originDataArray.addAll(arr);
                }
            }

            String windowStart = TimeHelper.Format.formatAsCommStr(context.window().getStart());
            String windowEnd = TimeHelper.Format.formatAsCommStr(context.window().getEnd());
            String currentWatermark = TimeHelper.Format.formatAsCommStr(context.currentWatermark());

            Row result = Row.of(key, windowStart, count, sum, windowEnd, currentWatermark, originDataArray);
            out.collect(result);
//            LOG.debug("WinAdd 窗口添加({}), key={}, row={}, accumulator={} ", dataStr, value.f0, value.f1, accumulator);
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




}
