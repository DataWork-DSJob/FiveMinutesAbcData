package flink.debug;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class FlinkDebugCommon {

    protected StreamExecutionEnvironment getStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000L);
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000L * 5); // 4秒生成1次水位

        return env;
    }

    public void runSimpleDemoJsonSource2WindowAgg2Print(StreamExecutionEnvironment env, Integer batch, Integer rate) {
        if (null == env) {
            env = getStreamEnv();
        }
        DataStreamSource<String> kafkaDataStream = env.addSource(new FixRateJsonStringSource(batch, rate, null));
        SingleOutputStreamOperator<String> outDS = transformString2Json2Watermark2Window(kafkaDataStream);
        outDS.print("OutPrint: ");
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

        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), kafkaProps);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafka);
        SingleOutputStreamOperator<String> outDS = transformString2Json2Watermark2Window(kafkaDataStream);
        outDS.print("OutPrint: ");
        try {
            env.execute(this.getClass().getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public void runFlinkKafka2KafkaDemo(StreamExecutionEnvironment env, String bootstrapServices, String inputTopic, String outputTopic, Properties kafkaProps) {
        if (null == kafkaProps) {
            kafkaProps = new Properties();
        }
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServices);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getSimpleName());

        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(inputTopic, new SimpleStringSchema(), kafkaProps);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafka);
        SingleOutputStreamOperator<String> outDS = transformString2Json2Watermark2Window(kafkaDataStream);

        outDS.addSink(new FlinkKafkaProducer<>(bootstrapServices, outputTopic, new SimpleStringSchema()));

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
            String groupKey = "groupKey_";
            try {
                String valueString = value.getString("groupKey");
                if (null == valueString) {
                    valueString = "groupKey_" + random.nextInt(randomSuffixNum);
                }
                groupKey = valueString;
            } catch (Exception e) {
                groupKey = "groupKey_" + random.nextInt(randomSuffixNum);
            }
            return groupKey;
        }
    }

    class MyJsonProcWindow extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {

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
    }

    private SingleOutputStreamOperator<String> transformString2Json2Watermark2Window(DataStreamSource<String> kafkaDataStream) {
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


}
