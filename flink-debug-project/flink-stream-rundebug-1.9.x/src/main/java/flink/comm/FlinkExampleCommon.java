package flink.comm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class FlinkExampleCommon {

    protected StreamExecutionEnvironment getStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000L * 5); // 4秒生成1次水位

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        return env;
    }


    protected void doFlinkKafkaStreamExample(ParameterTool param) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000L);
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,setting);
        LocalDateTime dayStart = LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toLocalDateTime();

        SingleOutputStreamOperator<Row> ds1 = env.fromElements(
                //      0              1                2                 3    4    5
                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(0).plusSeconds(1), 101010),
                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(2).plusSeconds(1), 102010),
                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(2).plusSeconds(23), 102020),
                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(12).plusSeconds(0), 104010),

                Row.of("TPerf08_deviceAst_2", dayStart.plusMinutes(20).plusSeconds(1), 201010),
                Row.of("TPerf08_deviceAst_2", dayStart.plusMinutes(22).plusSeconds(1), 202010),

                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(6).plusSeconds(32), 105010),
                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(12).plusSeconds(32), 104020),
                Row.of("TPerf08_deviceAst_1", dayStart.plusMinutes(21).plusSeconds(32), 106010)

        )
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        LocalDateTime timeField = (LocalDateTime) value.getField(1);
                        long time = timeField.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        Row row = Row.of(value.getField(0), time, value.getField(2), timeField.toLocalTime().toString());
                        return row;
                    }
                })
//                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        Object time = element.getField(1);
                        return (long) time;
                    }
                });
        System.out.println("ds1.parallelism = "+ ds1.getParallelism());


        SingleOutputStreamOperator<Row> window = ds1
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        Object category = value.getField(0);
                        return category.toString();
                    }
                })
                .timeWindow(Time.minutes(1))
                .apply(new WindowFunction<Row, Row, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Row> input, Collector<Row> out) throws Exception {
                        long start = window.getStart();
                        String lastLocalDate = null;
                        int count = 0;
                        Iterator<Row> it = input.iterator();
                        long lastTime = 0L;
                        double lastProduction = 0;
                        Object lastLocalTime = null;

                        while (it.hasNext()) {
                            Row row = it.next();
                            if (null != row) {
                                count++;
                                long time = (long) row.getField(1);
                                Object prodField = row.getField(2);
                                if (time > lastTime && null != prodField) {
                                    lastProduction = (int) prodField;
                                    lastTime = time;
                                    lastLocalTime = row.getField(3);
                                }
                            }
                        }

                        Row row = new Row(5);
                        row.setField(0, LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneId.systemDefault()));
                        row.setField(1, key);
                        row.setField(2, lastLocalTime);
                        row.setField(3, lastProduction);
                        row.setField(4, count);

                        out.collect(row);

                    }
                });

        window.print()

        ;

        System.out.println("window.parallelism = "+ window.getParallelism());

        try {
            env.execute(this.getClass().getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public void runFlinkKafkaConsumerDemo(StreamExecutionEnvironment env, String bootstrapServices, String topic, Properties kafkaProps) {
        if (null == kafkaProps) {
            kafkaProps = new Properties();
        }

        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServices);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getSimpleName());


        SimpleStringSchema stringSchema = new SimpleStringSchema();
        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(topic, stringSchema, kafkaProps);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafka);

        DataStream<JSONObject> ds = kafkaDataStream.map(line -> {
            JSONObject json = JSON.parseObject(line);
            json.put("timeMillis", System.currentTimeMillis());
            return json;
        });
        ds.addSink(new SinkFunction<JSONObject>() {
            private long lastPrintTime = 0L;
            @Override
            public void invoke(JSONObject value, Context context) throws Exception {
                long curTime = System.currentTimeMillis();
                Long timeMillis = value.getLong("timeMillis");
                if (null != timeMillis) {
                    long delta = timeMillis - curTime;
                    if (delta > 1000 * 60 * 10) {
                        if (curTime - lastPrintTime > 10000) {
                            System.out.println(Thread.currentThread().getName() + " -> " + value.toJSONString());
                            lastPrintTime = curTime;
                        }
                    }
                }
                if (curTime - lastPrintTime > 60000) {
                    System.out.println(Thread.currentThread().getName() + " -> " + value.toJSONString());
                    lastPrintTime = curTime;
                }
            }
        });

        try {
            env.execute(this.getClass().getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


    }


    static class MyKeySelector implements KeySelector<JSONObject, String> {
        private Random random = new Random();
        private int randomSuffixNum = 5;
        @Override
        public String getKey(JSONObject value) throws Exception {
            String groupKey = "groupKey_" + random.nextInt(randomSuffixNum);
            return groupKey;
        }
    }

    public void runFlinkKafka2KafkaDemo(StreamExecutionEnvironment env, String bootstrapServices, String inputTopic, String outputTopic, Properties kafkaProps) {
        if (null == kafkaProps) {
            kafkaProps = new Properties();
        }
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServices);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getSimpleName());

        SimpleStringSchema stringSchema = new SimpleStringSchema();
        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(inputTopic, stringSchema, kafkaProps);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafka);

        SingleOutputStreamOperator<String> outDS = kafkaDataStream
                .map(line -> {
                    JSONObject json = JSON.parseObject(line);
                    json.put("timeMillis", System.currentTimeMillis());
                    return json;
                })
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
                .map(line -> {
                    line.put("tid", Thread.currentThread().getId());
                    return line;
                })
                .keyBy(new MyKeySelector())
                .timeWindow(Time.minutes(1))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                        JSONObject json = new JSONObject();
                        json.put("key", key);
                        Iterator<JSONObject> it = elements.iterator();
                        int count = 0;
                        while (it.hasNext()) {
                            JSONObject data = it.next();
                            count++;
                        }

                        json.put("count", count);
                        out.collect(json);

                        Thread.sleep(500);
                    }
                })
                .map(k -> k.toJSONString());

        outDS.addSink(new FlinkKafkaProducer<>(bootstrapServices, outputTopic, new SimpleStringSchema()));

        try {
            env.execute(this.getClass().getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


    }


    public void testConsumerKafkaToJson(StreamExecutionEnvironment env, FlinkKafkaConsumer<String> kafkaConsumer) {

        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<String> outDS = kafkaDataStream
                .map(line -> {
                    JSONObject json;
                    try {
                        json = JSON.parseObject(line);

                    } catch (Exception e) {
                        json = new JSONObject();
                        json.put("line", line);
                    }
                    json.put("timeMillis", System.currentTimeMillis());
                    return json;
                })
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
                    return json;
                })
                .keyBy(new MyKeySelector())
                .timeWindow(Time.minutes(1))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                        JSONObject json = new JSONObject();
                        json.put("key", key);
                        Iterator<JSONObject> it = elements.iterator();
                        int count = 0;
                        while (it.hasNext()) {
                            JSONObject data = it.next();
                            count++;
                        }

                        json.put("count", count);
                        out.collect(json);

                        Thread.sleep(500);
                    }
                })
                .map(k -> k.toJSONString());

        outDS.print("TestPrintOut: ");

        try {
            env.execute(this.getClass().getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


    }


}
