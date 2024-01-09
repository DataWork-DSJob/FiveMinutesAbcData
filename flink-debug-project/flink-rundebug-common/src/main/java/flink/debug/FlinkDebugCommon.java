package flink.debug;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import flink.debug.base.BasePipeline4Test;
import flink.debug.base.FixRateJsonStringSource;
import flink.debug.base.MyJsonProcWindow;
import flink.debug.base.MyKeySelector;
import flink.debug.pressure.ParallismJsonStringSource;
import flink.debug.pressure.TrashSink;
import flink.debug.utils.CommonTestUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class FlinkDebugCommon extends BasePipeline4Test {

//    private SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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


    public String formatAsSimpleStr(long timeMillis) {
        String str = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timeMillis));
        return str;
    }

    public long parseBySimple(String timeStr) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeStr).getTime();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }


    private StreamExecutionEnvironment sEnv;

    protected StreamExecutionEnvironment getStreamEnv() {
        if (null == sEnv) {
            synchronized (this) {
                if (null == sEnv) {
                    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                    env.enableCheckpointing(1000 * 2);
//                    env.getCheckpointConfig().setCheckpointStorage("file:/D:/tmp/paimon/");

                    Path dirPath = getOrCreateDirFromUserDir("checkpoint-fs");
                    String backendPath = dirPath.toUri().toString();
                    env.setStateBackend(new FsStateBackend(backendPath));

                    env.setParallelism(1);
//                    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                    this.sEnv = env;
                }
            }
        }

//        env.getConfig().setAutoWatermarkInterval(1000L * 5); // 4秒生成1次水位
        return sEnv;
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


    public void runSimpleDemoJsonSource2WindowAgg2PrintWithEnv(Configuration configuration, Integer batch, Integer rate) {
        StreamExecutionEnvironment env = getStreamEnv();
        int defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM, 1);
        env.setParallelism(defaultParallelism);

        Integer numSlots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(numSlots)
                .build();

        JobGraph jobGraph = createSimpleDemoJsonSource2WindowAgg2PrintJobGraph(env, batch, rate);
        try {
            MiniCluster miniCluster = new MiniCluster(cfg);
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runSimpleTableDemoDatagen2WindowAgg2Print(StreamExecutionEnvironment env, Configuration configuration) {
        if (null == env) {
            env = getStreamEnv();
        }
        TableEnvironment tableEnv = StreamTableEnvironment.create(getStreamEnv());

        /*
CREATE CATALOG test_mem_catalog WITH (
 'type'='generic_in_memory'
)
;

USE CATALOG test_mem_catalog;
show tables;

SET 'execution.checkpointing.interval' = '10 s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

         */
        tableEnv.executeSql("CREATE CATALOG test_mem_catalog WITH (\n" +
                " 'type'='generic_in_memory'\n" +
                ")");
        tableEnv.executeSql("USE CATALOG test_mem_catalog");

        // 2.1 Datagen 模拟维表: dim_branch_info
        /*
CREATE TEMPORARY TABLE dim_branch_info (
branch_id INT PRIMARY KEY,
branch_name STRING,
branch_type INT,
province INT,
branch_manager STRING,
update_time TIMESTAMP(3),
WATERMARK FOR update_time AS update_time
) WITH (
'connector' = 'datagen',
'fields.branch_id.kind' = 'sequence',
'fields.branch_id.start' = '3001',
'fields.branch_id.end' = '3009',
'fields.branch_name.length' = '2',
'fields.branch_type.kind' = 'random',
'fields.branch_type.min' = '101',
'fields.branch_type.max' = '103',
'fields.province.kind' = 'random',
'fields.province.min' = '601',
'fields.province.max' = '609',
'fields.branch_manager.length' = '1'
)

         */
        tableEnv.executeSql("CREATE TEMPORARY TABLE dim_branch_info (\n" +
                "branch_id INT PRIMARY KEY,\n" +
                "branch_name STRING,\n" +
                "branch_type INT,\n" +
                "province INT,\n" +
                "branch_manager STRING,\n" +
                "update_time TIMESTAMP(3),\n" +
                "WATERMARK FOR update_time AS update_time\n" +
                ") WITH (\n" +
                "'connector' = 'datagen', \n" +
                "'fields.branch_id.kind' = 'sequence', \n" +
                "'fields.branch_id.start' = '3001', \n" +
                "'fields.branch_id.end' = '3009', \n" +
                "'fields.branch_name.length' = '2', \n" +
                "'fields.branch_type.kind' = 'random', \n" +
                "'fields.branch_type.min' = '101', \n" +
                "'fields.branch_type.max' = '103', \n" +
                "'fields.province.kind' = 'random', \n" +
                "'fields.province.min' = '601', \n" +
                "'fields.province.max' = '609', \n" +
                "'fields.branch_manager.length' = '1'\n" +
                ")");
        tableEnv.executeSql("SELECT * FROM dim_branch_info").print();


        // 3.1 构建 dwd_trade_detail 交易明细表;
        /*
 CREATE TEMPORARY TABLE dwd_trade_detail (
    trade_id BIGINT,
    trade_type INT,
    branch_id INT,
    trade_amount DOUBLE,
    result_code INT,
    trade_time AS localtimestamp,
    WATERMARK FOR trade_time AS trade_time
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.trade_id.kind' = 'sequence',
'fields.trade_id.start' = '1',
'fields.trade_id.end' = '10000',
'fields.trade_type.kind' = 'random',
'fields.trade_type.min' = '201',
'fields.trade_type.max' = '206',
'fields.branch_id.kind' = 'random',
'fields.branch_id.min' = '3001',
'fields.branch_id.max' = '3008',
'fields.trade_amount.kind' = 'random',
'fields.trade_amount.min' = '4000',
'fields.trade_amount.max' = '4002',
'fields.result_code.min' = '-1',
'fields.result_code.max' = '1'
)
         */

        tableEnv.executeSql("CREATE TEMPORARY TABLE dwd_trade_detail (\n" +
                "    trade_id BIGINT,\n" +
                "    trade_type INT,\n" +
                "    branch_id INT,\n" +
                "    trade_amount DOUBLE,\n" +
                "    result_code INT,\n" +
                "    trade_time AS localtimestamp,\n" +
                "    WATERMARK FOR trade_time AS trade_time\n" +
                ") WITH (\n" +
                "'connector' = 'datagen',\n" +
                "'rows-per-second' = '1',\n" +
                "'fields.trade_id.kind' = 'sequence',\n" +
                "'fields.trade_id.start' = '1',\n" +
                "'fields.trade_id.end' = '10000',\n" +
                "'fields.trade_type.kind' = 'random',\n" +
                "'fields.trade_type.min' = '201',\n" +
                "'fields.trade_type.max' = '206',\n" +
                "'fields.branch_id.kind' = 'random',\n" +
                "'fields.branch_id.min' = '3001',\n" +
                "'fields.branch_id.max' = '3008',\n" +
                "'fields.trade_amount.kind' = 'random',\n" +
                "'fields.trade_amount.min' = '4000',\n" +
                "'fields.trade_amount.max' = '4002',\n" +
                "'fields.result_code.min' = '-1',\n" +
                "'fields.result_code.max' = '1'\n" +
                ")");

//        tableEnv.executeSql("SELECT * FROM dwd_trade_detail")
//                .print()
//        ;

        // 4.1 简单 dws_trade_summary_1m
        /*
SELECT trade_type, branch_id, result_code,
count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,
       concat_ws('', CAST(trade_type AS STRING), CAST(branch_id AS STRING), CAST(result_code AS STRING)) pk
FROM dwd_trade_detail
GROUP BY trade_type, branch_id, result_code

         */
        tableEnv.executeSql("SELECT trade_type, branch_id, result_code,\n" +
                        "count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,\n" +
                        "       concat_ws('', CAST(trade_type AS STRING), CAST(branch_id AS STRING), CAST(result_code AS STRING)) pk\n" +
                        "FROM dwd_trade_detail\n" +
                        "GROUP BY trade_type, branch_id, result_code\n")
                        .print();

        // 4.2 简单 dws_trade_summary_1m
        /*
SELECT window_start,trade_type, branch_id, result_code,
       count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,
       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(trade_type AS STRING), CAST(branch_id AS STRING), CAST(result_code AS STRING)) pk
FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES))
GROUP BY window_start, trade_type, branch_id, result_code
;

         */
//        tableEnv.executeSql("SELECT window_start,trade_type, branch_id, result_code,\n" +
//                        "       count(*) cnt, sum(trade_amount) trade_amount_sum, PROCTIME() AS query_time,\n" +
//                        "       concat_ws('', DATE_FORMAT(window_start, 'yyyyMMddHH:mm:ss') , CAST(trade_type AS STRING), CAST(branch_id AS STRING), CAST(result_code AS STRING)) pk\n" +
//                        "FROM TABLE(TUMBLE(TABLE dwd_trade_detail, DESCRIPTOR(trade_time), INTERVAL '1' MINUTES))\n" +
//                        "GROUP BY window_start, trade_type, branch_id, result_code ")
//                .print();

        executeLocalJobByMiniCluster(env.getStreamGraph().getJobGraph(), configuration);

    }



    public void executeLocalJobByMiniCluster(JobGraph jobGraph, Configuration configuration) {
        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (null == configuration) {
            configuration = new Configuration();
        }
        Integer numSlots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        Integer tmNum = 1;
        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumTaskManagers(tmNum)
                .setNumSlotsPerTaskManager(numSlots)
                .build();
        try {
            MiniCluster miniCluster = new MiniCluster(cfg);
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public JobGraph createSimpleDemoJsonSource2WindowAgg2PrintJobGraph(StreamExecutionEnvironment env, Integer batch, Integer rate) {
        if (null == env) {
            env = getStreamEnv();
        }
        DataStreamSource<String> kafkaDataStream = env.addSource(new FixRateJsonStringSource(batch, rate, null));
        SingleOutputStreamOperator<String> outDS = transformString2Json2Watermark2Window(kafkaDataStream);

        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = streamGraph.getJobGraph();
        return jobGraph;
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
        outDS.addSink(new TrashSink());
//        outDS.print("OutPrint: ");
        try {
            env.execute(this.getClass().getSimpleName());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Data
    @AllArgsConstructor
    class PodInfo {
        String hostname;
        String ip;
        Long opTime;
    }

    public void testFlinkFunctions(StreamExecutionEnvironment env) {
        if (null == env) {
            env = getStreamEnv();
        }
        DataStream<String> stressSource = env
                .addSource(new ParallismJsonStringSource(null))
                .map(new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        return value.toJSONString();
                    }
                });

        SingleOutputStreamOperator<PodInfo> dimDStream = env.fromElements(
                        new PodInfo("hadoop01", "192.168.110.11", 0L),
                        new PodInfo("hadoop01", "192.168.110.11", 0L)
                )
                .flatMap((p, c) -> {
                    p.setOpTime(System.nanoTime());
                    c.collect(p);
                });



        SingleOutputStreamOperator<String> outDS = transformFlinkAllFunctions(stressSource);
        outDS.addSink(new TrashSink());
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


    private SingleOutputStreamOperator<String> transformFlinkAllFunctions(DataStream<String> kafkaDataStream) {

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
        }).print("中间jsonObjDStream 并列map打印: ");

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


}
