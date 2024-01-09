package flink.debug.stream;

/**
 * @projectName: FiveMinutesAbcData
 * @className: kafkaSourceTriggerTest
 * @description: flink.debug.stream.kafkaSourceTriggerTest
 * @author: jiaqing.he
 * @date: 2023/10/4 8:54
 * @version: 1.0
 */

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.debug.base.MyPunctuatedWatermarksAssignerAscendingTimestamp;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;

/*
trigger 测试
滚动窗口，20s
然后是trigger内部技术，10个元素输出一次。

*/
public class FlinkTriggerEvictorDemo implements Serializable {
    private static final long serialVersionUID = 1L;

    @Test
    public void testTrigger() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> lines = env.socketTextStream("bdnode112", 9909);
        lines.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        })
                .timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
                // Count Trigger
                .trigger(CountTrigger.of(10))
                .trigger(new Trigger<Object, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    private int count = 0;
                    private static final int MAX_COUNT = 5;
                    //每个元素被添加到窗口时都会调用该方法
                    @Override
                    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 这个是干嘛?
                        ctx.registerProcessingTimeTimer(window.maxTimestamp());
                        // CONTINUE是代表不做输出，也即是，此时我们想要实现比如100条输出一次，
                        // 而不是窗口结束再输出就可以在这里实现。
                        TriggerResult triggerResult;
                        count++;
                        if(count > MAX_COUNT){
                            count = 0;
                            triggerResult = TriggerResult.FIRE;
                        } else {
                            triggerResult = TriggerResult.CONTINUE;
                        }
                        System.out.println("onElement : "+element);
                        return triggerResult;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
                    }

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) {
                        // only register a timer if the time is not yet past the end of the merged window
                        // this is in line with the logic in onElement(). If the time is past the end of
                        // the window onElement() will fire and setting a timer here would fire the window twice.
                        long windowMaxTimestamp = window.maxTimestamp();
                        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
                        }
                    }

                })
                .sum(0)
                .print()
        ;


        env.execute("Flink Streaming Java API Skeleton");
    }
    public Path getOrCreateDirFromUserDir(String dirName) {
        if (null == dirName || dirName.isEmpty()) {
            throw new IllegalArgumentException("dirName cannot be null");
        }
        Path dirPath = Paths.get(System.getProperty("user.dir"), dirName);
        if (!Files.exists(dirPath)) {
            try {
                dirPath = Files.createDirectory(dirPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return dirPath.toAbsolutePath();
    }

    @Test
    public void testTriggerV2() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableChangelogStateBackend(true);
        Path dirPath = getOrCreateDirFromUserDir("checkpoint-rocksdb");
        env.setStateBackend(new RocksDBStateBackend(dirPath.toUri().toString()));
        env.setStateBackend(new FsStateBackend(dirPath.toUri().toString()));
        env.enableCheckpointing(1000*3, CheckpointingMode.EXACTLY_ONCE);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> lines = env.socketTextStream("bdnode112", 9909);

        lines
                .flatMap(new FlatMapFunction<String, Row>() {
                    @Override
                    public void flatMap(String line, Collector<Row> out) throws Exception {
                        if (null != line && !line.isEmpty()) {
                            String[] split = line.split(",");
                            if (split.length > 0) {
                                Row row;
                                if (split.length == 1) {
                                    String numStr = split[0].trim();
                                    Object num;
                                    try {
                                        num = Integer.parseInt(numStr);
                                    } catch (RuntimeException e) {
                                        try {
                                            num = new BigDecimal(numStr).doubleValue();
                                        } catch (RuntimeException e2) {
                                            num = numStr;
                                        }
                                    }
                                    row = Row.of(num);
                                } else {
                                    // 有key,
                                    row = new Row(split.length);
                                    for (int i = 0; i < split.length; i++) {
                                        String ele = split[i];
                                        row.setField(i, ele);
                                    }
                                }

                                // 输出
                                try {
                                    out.collect(row);
                                } catch (RuntimeException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Row>forMonotonousTimestamps()
//                        .withTimestampAssigner((e,t) -> System.currentTimeMillis()))
//                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks())
                .assignTimestampsAndWatermarks(new MyPunctuatedWatermarksAssignerAscendingTimestamp())
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Row>() {
                    @Override
                    public long extractTimestamp(Row element, long recordTimestamp) {
                        return 0;
                    }

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Row lastElement, long extractedTimestamp) {
                        return null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<Row>forBoundedOutOfOrderness(Duration.ofMillis(1000*5))
                                .withTimestampAssigner((event, time) -> {
                                    long ts = System.currentTimeMillis();
                                    if (event.getArity() > 1) {
                                        Object secondAsTs = event.getField(1);
                                        if (null != secondAsTs) {
                                            if (secondAsTs instanceof Number) {
                                                ts = (long) ((Number) secondAsTs).doubleValue();
                                            } else if (secondAsTs instanceof String) {
                                                try {
                                                    LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
                                                    ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
                                                } catch (RuntimeException e) {
                                                    e.printStackTrace();
                                                }
                                            }
                                        }
                                    }
                                    return ts;
                                })
                )
//                .assignTimestampsAndWatermarks()
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        String pk;
                        if (row.getArity() <= 1) {
                            pk = "";
                        } else {
                            Object firstValue = row.getField(0);
                            if (null != firstValue && firstValue instanceof String) {
                                pk = (String) firstValue;
                            } else {
                                // 若第一个元素 字符串, 则未设置分组, 使用 统一默认 分组;
                                pk = "";
                            }
                        }
                        return pk;
                    }
                })
//                .window(TumblingProcessingTimeWindows.of(
//                        Time.seconds(10),
//                        Time.seconds(0),// 窗口从偏移多长开始; 如 (1h,15m)则 start at 0:15:00,1:15:00,2:15:00,etc
//                        //ALIGNED 对其:  all panes fire at the same time across all partitions.
//                        // RANDOM :
//                        WindowStagger.ALIGNED) //  The utility that produces staggering offset  交错 staggers offset for each window assignment
//                )

                .window(TumblingEventTimeWindows.of(
                        Time.seconds(10), Time.seconds(0),
                        WindowStagger.ALIGNED
                ))
//                .trigger(CountTrigger.of(3))
                .trigger(new Trigger<Object, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    private int count = 0;
                    private static final int MAX_COUNT = 3;
                    //每个元素被添加到窗口时都会调用该方法
                    @Override
                    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 这个是干嘛?
//                        ctx.registerProcessingTimeTimer(window.maxTimestamp());
                        // CONTINUE是代表不做输出，也即是，此时我们想要实现比如100条输出一次，
                        // 而不是窗口结束再输出就可以在这里实现。
                        TriggerResult triggerResult;
                        count++;
                        if(count > MAX_COUNT){
                            count = 0;
                            triggerResult = TriggerResult.FIRE;
                        } else {
                            triggerResult = TriggerResult.CONTINUE;
                        }

                        System.out.println(String.format("onElement, tid=%d, curWm=%s, winStart=%s, count=%d; \t Record=%s",
                                Thread.currentThread().getId(),
                                new Date(ctx.getCurrentWatermark()).toLocaleString(),
                                new Date(window.getStart()).toLocaleString(),
                                count,
                                element
                        ));
                        return triggerResult;
                    }

                    // registerProcessingTimeTimer() ? 注册的系统时间计时器触发时，将调用onEventTime（）方法;  Called when a processing-time timer that was set using the trigger context fires
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        String curWm = new Date(ctx.getCurrentWatermark()).toLocaleString();
                        String curTime = new Date().toLocaleString();

                        System.out.println(String.format("Window.onEventTime(水位更新,或者注册实际到), tid=%d, curWm=%s, curTime=%s",
                                Thread.currentThread().getId(),
                                curWm, curTime
                        ));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
                    }

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) {
                        // only register a timer if the time is not yet past the end of the merged window
                        // this is in line with the logic in onElement(). If the time is past the end of
                        // the window onElement() will fire and setting a timer here would fire the window twice.
                        long windowMaxTimestamp = window.maxTimestamp();
                        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
                        }
                    }

                })
                .aggregate(new AggregateFunction<Row, JSONObject, Row>() {
                    @Override
                    public JSONObject createAccumulator() {
                        JSONObject acc = new JSONObject();
                        acc.put("count", 0L);
                        return acc;
                    }

                    @Override
                    public JSONObject add(Row value, JSONObject accumulator) {
                        if (null != value) {
                            long count = accumulator.getLongValue("count");
                            count++;
                            accumulator.put("count",count);
                        }
                        return accumulator;
                    }

                    @Override
                    public Row getResult(JSONObject accumulator) {
                        long count = accumulator.getLongValue("count");
                        return Row.of(count);
                    }

                    @Override
                    public JSONObject merge(JSONObject a, JSONObject b) {
                        a.put("count", a.getLongValue("count") + b.getLongValue("count"));
                        return a;
                    }
                }, new ProcessWindowFunction<Row, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Row, JSONObject, String, TimeWindow>.Context context, Iterable<Row> elements, Collector<JSONObject> out) throws Exception {
                        Iterator<Row> it = elements.iterator();
                        while (it.hasNext()) {
                            Row row = it.next();
                            JSONObject outRecord = new JSONObject();
                            outRecord.put("groupKey", key);
                            outRecord.put("curWatermark", context.currentWatermark());
                            outRecord.put("win_start", new Date(context.window().getStart()).toLocaleString());

                            for (int i = 0; i < row.getArity(); i++) {
                                Object fValue = row.getField(i);
                                outRecord.put("rowIdx_" + i, fValue);
                            }
                            out.collect(outRecord);
                        }
                    }
                })
                .print()
        ;

        env.execute("Flink Streaming Java API Skeleton");
    }

    /**
     *  测试 Trigger 和 Watermark,
     *  自定义了 WatermarkGenerator, 其 onEvent() 从每条数据收集最大timestamp到成员变量(当并行), 其 onPeriodicEmit() 定期(200ms)发送新水位;
     *  自定义了 TimestampAssigner , 其 extractTimestamp() 提取 时间戳long 作为系统给每条数据的 timestamp;
     *
     *  自定义实现 Trigger:
     *      - onElement() 在当 并行内计数, 如果超过3就 Fire并重新计数;
     *      - 其 onEventTime() 由 全局水位更新 并触发 WindowOperator.onEventTime() 调用触发; 可决定是否输出;
     *
     * @throws Exception
     */
    @Test
    public void testWatermark() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableChangelogStateBackend(true);
        Path dirPath = getOrCreateDirFromUserDir("checkpoint-rocksdb");
        env.setStateBackend(new RocksDBStateBackend(dirPath.toUri().toString()));
        env.setStateBackend(new FsStateBackend(dirPath.toUri().toString()));
        env.enableCheckpointing(1000*3, CheckpointingMode.EXACTLY_ONCE);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // A,2023-10-04T23:07:15,1
        DataStreamSource<String> lines = env.socketTextStream("bdnode112", 9909);

        lines
                .flatMap(new FlatMapFunction<String, Row>() {
                    @Override
                    public void flatMap(String line, Collector<Row> out) throws Exception {
                        if (null != line && !line.isEmpty()) {
                            String[] split = line.split(",");
                            if (split.length > 0) {
                                Row row;
                                if (split.length == 1) {
                                    String numStr = split[0].trim();
                                    Object num;
                                    try {
                                        num = Integer.parseInt(numStr);
                                    } catch (RuntimeException e) {
                                        try {
                                            num = new BigDecimal(numStr).doubleValue();
                                        } catch (RuntimeException e2) {
                                            num = numStr;
                                        }
                                    }
                                    row = Row.of(num);
                                } else {
                                    // 有key,
                                    row = new Row(split.length);
                                    for (int i = 0; i < split.length; i++) {
                                        String ele = split[i];
                                        row.setField(i, ele);
                                    }
                                }
                                // 输出
                                try {
                                    out.collect(row);
                                } catch (RuntimeException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Row>forMonotonousTimestamps()
//                        .withTimestampAssigner((e,t) -> System.currentTimeMillis()))
//                .assignTimestampsAndWatermarks(new MyPunctuatedWatermarksAssignerAscendingTimestamp())
//                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Row>() {
//                    @Override
//                    public long extractTimestamp(Row element, long recordTimestamp) {
//                        return 0;
//                    }
//
//                    @Nullable
//                    @Override
//                    public Watermark checkAndGetNextWatermark(Row lastElement, long extractedTimestamp) {
//                        return null;
//                    }
//                })
//              //  容忍乱序数据, 提取 timestamp
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Row>forBoundedOutOfOrderness(Duration.ofMillis(1000*5))
//                        .withTimestampAssigner((event, time) -> {
//                            long ts = System.currentTimeMillis();
//                            if (event.getArity() > 1) {
//                                Object secondAsTs = event.getField(1);
//                                if (null != secondAsTs) {
//                                    if (secondAsTs instanceof Number) {
//                                        ts = (long) ((Number) secondAsTs).doubleValue();
//                                    } else if (secondAsTs instanceof String) {
//                                        try {
//                                            LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
//                                            ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
//                                        } catch (RuntimeException e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
//                                }
//                            }
//                            return ts;
//                        })
//                )
                // 完全自定义 WatermarkGenerator  & TimestampAssigner
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Row>() {
                    @Override
                    public WatermarkGenerator<Row> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Row>() {
                            private final long outOfOrdernessMillis = 1000 * 0;
                            private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
                            @Override
                            public void onEvent(Row event, long eventTimestamp, WatermarkOutput output) {
                                if (eventTimestamp > maxTimestamp) {
                                    System.out.println(String.format("tid=%d 最大时间戳将更新(from %s to %s) \t event=%s",
                                            Thread.currentThread().getId(),
                                            new Date(maxTimestamp).toLocaleString(),
                                            new Date(eventTimestamp).toLocaleString(),
                                            event
                                    ));
                                }
                                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            }
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                long curWatermark = maxTimestamp - outOfOrdernessMillis - 1;
                                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(curWatermark));
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<Row> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<Row>() {
                            @Override
                            public long extractTimestamp(Row event, long recordTimestamp) {
                                long ts = System.currentTimeMillis();
                                if (event.getArity() > 1) {
                                    Object secondAsTs = event.getField(1);
                                    if (null != secondAsTs) {
                                        if (secondAsTs instanceof Number) {
                                            ts = (long) ((Number) secondAsTs).doubleValue();
                                        } else if (secondAsTs instanceof String) {
                                            try {
                                                LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
                                                ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
                                            } catch (RuntimeException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }
                                }
                                return ts;
                            }
                        };
                    }
                })
               // // 无水位, 不过期
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Row>noWatermarks()
//                        .withTimestampAssigner((event, time) -> {
//                            long ts = System.currentTimeMillis();
//                            if (event.getArity() > 1) {
//                                Object secondAsTs = event.getField(1);
//                                if (null != secondAsTs) {
//                                    if (secondAsTs instanceof Number) {
//                                        ts = (long) ((Number) secondAsTs).doubleValue();
//                                    } else if (secondAsTs instanceof String) {
//                                        try {
//                                            LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
//                                            ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
//                                        } catch (RuntimeException e) {
//                                            e.printStackTrace();
//                                        }
//                                    }
//                                }
//                            }
//                            return ts;
//                        })
//                )

                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        String pk;
                        if (row.getArity() <= 1) {
                            pk = "";
                        } else {
                            Object firstValue = row.getField(0);
                            if (null != firstValue && firstValue instanceof String) {
                                pk = (String) firstValue;
                            } else {
                                // 若第一个元素 字符串, 则未设置分组, 使用 统一默认 分组;
                                pk = "";
                            }
                        }
                        return pk;
                    }
                })
//                .window(TumblingProcessingTimeWindows.of(
//                        Time.seconds(10),
//                        Time.seconds(0),// 窗口从偏移多长开始; 如 (1h,15m)则 start at 0:15:00,1:15:00,2:15:00,etc
//                        //ALIGNED 对其:  all panes fire at the same time across all partitions.
//                        // RANDOM :
//                        WindowStagger.ALIGNED) //  The utility that produces staggering offset  交错 staggers offset for each window assignment
//                )

                .window(TumblingEventTimeWindows.of(
                        Time.seconds(10), Time.seconds(0),
                        WindowStagger.ALIGNED
                ))
//                .trigger(CountTrigger.of(3))
                .trigger(new Trigger<Row, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    private int count = 0;
                    private static final int MAX_COUNT = 3;
                    //每个元素被添加到窗口时都会调用该方法
                    @Override
                    public TriggerResult onElement(Row element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 这个是干嘛?
                        ctx.registerEventTimeTimer(window.maxTimestamp());
                        // CONTINUE是代表不做输出，也即是，此时我们想要实现比如100条输出一次，
                        // 而不是窗口结束再输出就可以在这里实现。
                        TriggerResult triggerResult;
                        count++;
                        if(count > MAX_COUNT){
                            count = 0;
                            triggerResult = TriggerResult.FIRE;
                        } else {
                            triggerResult = TriggerResult.CONTINUE;
                        }

                        System.out.println(String.format("onElement, tid=%d, curWm=%s, winStart=%s, count=%d, trigger=%s; \t Record=%s",
                                Thread.currentThread().getId(),
                                new Date(ctx.getCurrentWatermark()).toLocaleString(),
                                new Date(window.getStart()).toLocaleString(),
                                count,
                                this,
                                element
                        ));
                        return triggerResult;
                    }

                    // registerProcessingTimeTimer() ? 注册的系统时间计时器触发时，将调用onEventTime（）方法;  Called when a processing-time timer that was set using the trigger context fires
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        String curWm = new Date(ctx.getCurrentWatermark()).toLocaleString();
                        String curTime = new Date().toLocaleString();

                        System.out.println(String.format("Window.onEventTime(水位更新,或者注册实际到), tid=%d, curWm=%s, curTime=%s",
                                Thread.currentThread().getId(),
                                curWm, curTime
                        ));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
                    }

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) {
                        // only register a timer if the time is not yet past the end of the merged window
                        // this is in line with the logic in onElement(). If the time is past the end of
                        // the window onElement() will fire and setting a timer here would fire the window twice.
                        long windowMaxTimestamp = window.maxTimestamp();
                        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
                        }
                    }

                })
                .aggregate(new AggregateFunction<Row, JSONObject, Row>() {
                    @Override
                    public JSONObject createAccumulator() {
                        JSONObject acc = new JSONObject();
                        acc.put("count", 0L);
                        acc.put("elementList", new JSONArray());
                        return acc;
                    }

                    @Override
                    public JSONObject add(Row value, JSONObject accumulator) {
                        if (null != value) {
                            long count = accumulator.getLongValue("count");
                            count++;
                            accumulator.put("count",count);
                            if (value.getArity() > 2) {
                                JSONArray elementList = accumulator.getJSONArray("elementList");
                                if (null == elementList) {
                                    elementList = new JSONArray();
                                    accumulator.put("elementList", elementList);
                                }
                                elementList.add(value.getField(2));
                            }
                        }
                        return accumulator;
                    }

                    @Override
                    public Row getResult(JSONObject accumulator) {
                        long count = accumulator.getLongValue("count");
                        JSONArray elementList = accumulator.getJSONArray("elementList");
                        return Row.of(count, elementList);
                    }

                    @Override
                    public JSONObject merge(JSONObject a, JSONObject b) {
                        a.put("count", a.getLongValue("count") + b.getLongValue("count"));
                        return a;
                    }
                }, new ProcessWindowFunction<Row, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Row, JSONObject, String, TimeWindow>.Context context, Iterable<Row> elements, Collector<JSONObject> out) throws Exception {
                        Iterator<Row> it = elements.iterator();
                        while (it.hasNext()) {
                            Row row = it.next();
                            JSONObject outRecord = new JSONObject();
                            outRecord.put("groupKey", key);
                            outRecord.put("curWatermark", context.currentWatermark() +" : " + new Date(context.currentWatermark()).toLocaleString());
                            outRecord.put("curProcTime", context.currentProcessingTime() +" : " + new Date(context.currentProcessingTime()).toLocaleString());
                            outRecord.put("win_start", new Date(context.window().getStart()).toLocaleString());

                            for (int i = 0; i < row.getArity(); i++) {
                                Object fValue = row.getField(i);
                                outRecord.put("rowIdx_" + i, fValue);
                            }
                            out.collect(outRecord);
                        }
                    }
                })
                .print()
        ;

        env.execute("Flink Streaming Java API Skeleton");
    }

    /**
     * Evictor 逐出的功能, 是指 emitWindowContents() 执行 userFunction.process() 前后, 剔除已聚合的元素,使其不参与 聚合计算输出和 状态缓存
     *  - evictBefore() , 在userFunction.process()前执行, 可移除已聚合数据不参与 聚合计算, 也不会再被存为状态;
     *  - evictAfter(), 在userFunction.process() 后面执行, 时元素不再被存为状态;
     * @throws Exception
     */
    @Test
    public void testEvictor() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableChangelogStateBackend(true);
        Path dirPath = getOrCreateDirFromUserDir("checkpoint-rocksdb");
        env.setStateBackend(new RocksDBStateBackend(dirPath.toUri().toString()));
        env.setStateBackend(new FsStateBackend(dirPath.toUri().toString()));
        env.enableCheckpointing(1000*3, CheckpointingMode.EXACTLY_ONCE);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // A,2023-10-04T23:07:15,1
        DataStreamSource<String> lines = env.socketTextStream("bdnode112", 9909);

        lines
                .flatMap(new FlatMapFunction<String, Row>() {
                    @Override
                    public void flatMap(String line, Collector<Row> out) throws Exception {
                        if (null != line && !line.isEmpty()) {
                            String[] split = line.split(",");
                            if (split.length > 0) {
                                Row row;
                                if (split.length == 1) {
                                    String numStr = split[0].trim();
                                    Object num;
                                    try {
                                        num = Integer.parseInt(numStr);
                                    } catch (RuntimeException e) {
                                        try {
                                            num = new BigDecimal(numStr).doubleValue();
                                        } catch (RuntimeException e2) {
                                            num = numStr;
                                        }
                                    }
                                    row = Row.of(num);
                                } else {
                                    // 有key,
                                    row = new Row(split.length);
                                    for (int i = 0; i < split.length; i++) {
                                        String ele = split[i];
                                        row.setField(i, ele);
                                    }
                                }
                                // 输出
                                try {
                                    out.collect(row);
                                } catch (RuntimeException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                })
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Row>() {
                    @Override
                    public WatermarkGenerator<Row> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Row>() {
                            private final long outOfOrdernessMillis = 1000 * 0;
                            private long maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
                            @Override
                            public void onEvent(Row event, long eventTimestamp, WatermarkOutput output) {
                                if (eventTimestamp > maxTimestamp) {
                                    System.out.println(String.format("tid=%d 最大时间戳将更新(from %s to %s) \t event=%s",
                                            Thread.currentThread().getId(),
                                            new Date(maxTimestamp).toLocaleString(),
                                            new Date(eventTimestamp).toLocaleString(),
                                            event
                                    ));
                                }
                                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            }
                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                long curWatermark = maxTimestamp - outOfOrdernessMillis - 1;
                                output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(curWatermark));
                            }
                        };
                    }

                    @Override
                    public TimestampAssigner<Row> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return new TimestampAssigner<Row>() {
                            @Override
                            public long extractTimestamp(Row event, long recordTimestamp) {
                                long ts = System.currentTimeMillis();
                                if (event.getArity() > 1) {
                                    Object secondAsTs = event.getField(1);
                                    if (null != secondAsTs) {
                                        if (secondAsTs instanceof Number) {
                                            ts = (long) ((Number) secondAsTs).doubleValue();
                                        } else if (secondAsTs instanceof String) {
                                            try {
                                                LocalDateTime parse = LocalDateTime.parse((String) secondAsTs);
                                                ts = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
                                            } catch (RuntimeException e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }
                                }
                                return ts;
                            }
                        };
                    }
                })
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        String pk;
                        if (row.getArity() <= 1) {
                            pk = "";
                        } else {
                            Object firstValue = row.getField(0);
                            if (null != firstValue && firstValue instanceof String) {
                                pk = (String) firstValue;
                            } else {
                                // 若第一个元素 字符串, 则未设置分组, 使用 统一默认 分组;
                                pk = "";
                            }
                        }
                        return pk;
                    }
                })
                .window(TumblingEventTimeWindows.of(
                        Time.seconds(10), Time.seconds(0),
                        WindowStagger.ALIGNED
                ))

                // // 设置了 evictor后, 就会创建和使用 EvictingWindowOperator 作为处理算子;
//                .evictor(new TimeEvictor(1000 * 10))
                .evictor(new Evictor<Object, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    private final long windowSize = 1000 * 5;
                    private final boolean doEvictAfter = false;

                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
                        if (!doEvictAfter) {
                            evict(elements, size, ctx);
                        }
                    }
                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
                        if (doEvictAfter) {
                            evict(elements, size, ctx);
                        }
                    }


                    private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
                        if (!hasTimestamp(elements)) {
                            return;
                        }

                        long currentTime = getMaxTimestamp(elements);
                        // 所有数据中 最大时间 - 设置的容许延迟间隔(windowSize) 为逐出标准, 小于该时间即 移除(不参与计算?)
                        long evictCutoff = currentTime - windowSize;

                        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
                             iterator.hasNext(); ) {
                            TimestampedValue<Object> record = iterator.next();
                            // 小于该时间即从 elements: Iterable<TimestampedValue<Object>> elements 中移除?
                            if (record.getTimestamp() <= evictCutoff) {
                                iterator.remove();
                            }
                        }
                    }

                    /**
                     * Returns true if the first element in the Iterable of {@link TimestampedValue} has a
                     * timestamp.
                     */
                    private boolean hasTimestamp(Iterable<TimestampedValue<Object>> elements) {
                        Iterator<TimestampedValue<Object>> it = elements.iterator();
                        if (it.hasNext()) {
                            return it.next().hasTimestamp();
                        }
                        return false;
                    }

                    /**
                     * @param elements The elements currently in the pane.
                     * @return The maximum value of timestamp among the elements.
                     */
                    private long getMaxTimestamp(Iterable<TimestampedValue<Object>> elements) {
                        long currentTime = Long.MIN_VALUE;
                        for (Iterator<TimestampedValue<Object>> iterator = elements.iterator();
                             iterator.hasNext(); ) {
                            TimestampedValue<Object> record = iterator.next();
                            currentTime = Math.max(currentTime, record.getTimestamp());
                        }
                        return currentTime;
                    }


                })
                .trigger(new Trigger<Row, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    private int count = 0;
                    private static final int MAX_COUNT = 3;
                    //每个元素被添加到窗口时都会调用该方法
                    @Override
                    public TriggerResult onElement(Row element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 这个是干嘛?
                        ctx.registerEventTimeTimer(window.maxTimestamp());
                        // CONTINUE是代表不做输出，也即是，此时我们想要实现比如100条输出一次，
                        // 而不是窗口结束再输出就可以在这里实现。
                        TriggerResult triggerResult;
                        count++;
                        if(count > MAX_COUNT){
                            count = 0;
                            triggerResult = TriggerResult.FIRE;
                        } else {
                            triggerResult = TriggerResult.CONTINUE;
                        }

                        System.out.println(String.format("onElement, tid=%d, curWm=%s, winStart=%s, count=%d, trigger=%s; \t Record=%s",
                                Thread.currentThread().getId(),
                                new Date(ctx.getCurrentWatermark()).toLocaleString(),
                                new Date(window.getStart()).toLocaleString(),
                                count,
                                this,
                                element
                        ));
                        return triggerResult;
                    }

                    // registerProcessingTimeTimer() ? 注册的系统时间计时器触发时，将调用onEventTime（）方法;  Called when a processing-time timer that was set using the trigger context fires
                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        String curWm = new Date(ctx.getCurrentWatermark()).toLocaleString();
                        String curTime = new Date().toLocaleString();

                        System.out.println(String.format("Window.onEventTime(水位更新,或者注册实际到), tid=%d, curWm=%s, curTime=%s",
                                Thread.currentThread().getId(),
                                curWm, curTime
                        ));
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
                    }

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) {
                        long windowMaxTimestamp = window.maxTimestamp();
                        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
                        }
                    }

                })
                .aggregate(new AggregateFunction<Row, JSONObject, Row>() {
                    @Override
                    public JSONObject createAccumulator() {
                        JSONObject acc = new JSONObject();
                        acc.put("count", 0L);
                        acc.put("elementList", new JSONArray());
                        return acc;
                    }

                    @Override
                    public JSONObject add(Row value, JSONObject accumulator) {
                        if (null != value) {
                            long count = accumulator.getLongValue("count");
                            count++;
                            accumulator.put("count",count);
                            if (value.getArity() > 2) {
                                JSONArray elementList = accumulator.getJSONArray("elementList");
                                if (null == elementList) {
                                    elementList = new JSONArray();
                                    accumulator.put("elementList", elementList);
                                }
                                elementList.add(value.getField(2));
                            }
                        }
                        return accumulator;
                    }

                    @Override
                    public Row getResult(JSONObject accumulator) {
                        long count = accumulator.getLongValue("count");
                        JSONArray elementList = accumulator.getJSONArray("elementList");
                        return Row.of(count, elementList);
                    }

                    @Override
                    public JSONObject merge(JSONObject a, JSONObject b) {
                        a.put("count", a.getLongValue("count") + b.getLongValue("count"));
                        return a;
                    }
                }, new ProcessWindowFunction<Row, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<Row, JSONObject, String, TimeWindow>.Context context, Iterable<Row> elements, Collector<JSONObject> out) throws Exception {
                        Iterator<Row> it = elements.iterator();
                        while (it.hasNext()) {
                            Row row = it.next();
                            JSONObject outRecord = new JSONObject();
                            outRecord.put("groupKey", key);
                            outRecord.put("curWatermark", context.currentWatermark() +" : " + new Date(context.currentWatermark()).toLocaleString());
                            outRecord.put("curProcTime", context.currentProcessingTime() +" : " + new Date(context.currentProcessingTime()).toLocaleString());
                            outRecord.put("win_start", new Date(context.window().getStart()).toLocaleString());

                            for (int i = 0; i < row.getArity(); i++) {
                                Object fValue = row.getField(i);
                                outRecord.put("rowIdx_" + i, fValue);
                            }
                            out.collect(outRecord);
                        }
                    }
                })
                .print()
        ;

        env.execute("Flink Streaming Java API Skeleton");
    }



}