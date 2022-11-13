package flink.debug;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import flink.debug.pressure.ParallismJsonStringSource;
import flink.debug.pressure.ParallismTrashSink;
import flink.debug.utils.CommonTestUtils;
import flink.debug.utils.DebugCommMethod;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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

import java.io.File;
import java.net.URL;
import java.util.*;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class FlinkDebugCommon extends DebugCommMethod {

    protected StreamExecutionEnvironment getStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000L);
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000L * 5); // 4秒生成1次水位

        return env;
    }

    public void setClasspathResourceAsJvmEnv(String envName, String flinkConfDir) {
        URL url = getClass().getClassLoader().getResource(flinkConfDir);
        if (null == url) {
            throw new IllegalArgumentException("Not found in Classpath: " + envName + "="+ flinkConfDir);
        }
        File dir = new File(url.getPath());
        if (!dir.exists()) {
            throw new IllegalArgumentException("Not found in Classpath: " + envName + "="+ flinkConfDir);
        }
        String confDirPath = dir.getAbsolutePath();
        Map<String, String> newEnvs = new HashMap(2);
        newEnvs.put(envName, confDirPath);
        CommonTestUtils.setEnv(newEnvs, false);
        String confDir = System.getenv(envName);
        if (null != confDir) {
            System.out.println("Succeed set ENV: " + envName + "=" + confDir);
        }

    }

    public void printFlinkEnv() {

        String hadoopHome = System.getenv("HADOOP_HOME");
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        String classpath = System.getenv("CLASSPATH");
        String flinkHome = System.getenv("FLINK_HOME");
        String flinkConfDir = System.getenv("FLINK_CONF_DIR");

        System.out.println("HADOOP_HOME= " + hadoopHome);
        System.out.println("HADOOP_CONF_DIR= " + hadoopConfDir);
        System.out.println("CLASSPATH= " + classpath);
        System.out.println("flinkHome= " + flinkHome);
        System.out.println("flinkConfDir= " + flinkConfDir);

        String hadoopConfDirKv = System.getProperty("HADOOP_CONF_DIR");
        System.out.println("hadoopConfDirKv= " + hadoopConfDirKv);

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

    public void pressureTest(StreamExecutionEnvironment env) {
        if (null == env) {
            env = getStreamEnv();
        }
        DataStream<String> kafkaDataStream = env
                .addSource(new ParallismJsonStringSource(null))
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        return value.toJSONString();
                    }
                });
        SingleOutputStreamOperator<String> outDS = transformString2Json2Watermark2Window(kafkaDataStream);
        outDS.addSink(new ParallismTrashSink());
//        outDS.print("OutPrint: ");
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

    private SingleOutputStreamOperator<String> transformString2Json2Watermark2Window(DataStream<String> kafkaDataStream) {
        SingleOutputStreamOperator<String> outDS = kafkaDataStream
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
