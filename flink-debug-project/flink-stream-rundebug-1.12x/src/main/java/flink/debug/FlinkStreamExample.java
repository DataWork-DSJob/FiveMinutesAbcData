package flink.debug;


import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Properties;

public class FlinkStreamExample extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.51.124:9092";


    @Test
    public void simpleDemoByF112() throws Exception {
        runSimpleDemoJsonSource2WindowAgg2Print(null, -1, 100000);
    }


    @Test
    public void testKafkaSource() throws Exception {
        StreamExecutionEnvironment streamEnv = getStreamEnv();
        Properties  kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        runFlinkKafkaConsumerDemo(streamEnv, bootstrapServers, "testSourceTopic", null);
    }

    @Test
    public void testFlinkKafka2KafkaDemo() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(0L);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties  kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        kafkaProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

//        runFlinkKafkaConsumerDemo(env, "bdnode102:9092", "testSourceTopic", kafkaProps);
        runFlinkKafka2KafkaDemo(env, bootstrapServers, "testSourceTopic", "testSinkTopic", kafkaProps);

    }

    @Test
    public void testFlinkFunctionsByFlink112() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        testFlinkFunctions(env);
    }



}
