package flink.comm;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Properties;

public class TestJaxKafkaByteSourceJob extends FlinkExampleCommon {

    public static void main(String[] args) {

        ParameterTool param = ParameterTool.fromArgs(args);

//        String bootstrapServices = param.get("bootstrapServices", "localhost:9092");
//        String topic = param.get("topic", "testRecords");

        new TestJaxKafkaByteSourceJob().doFlinkKafkaStreamExample(param);

    }


    private void setKafkaConsumer(String offsetModel, final boolean enableCommitOnCheckpoint,  FlinkKafkaConsumer<?> kafkaConsumer) {
        /*
            Flink kafka consumer支持三种offset， group, latest, earliest.
         */
        switch (offsetModel.toUpperCase()) {
            case "EARLIEST":
                kafkaConsumer.setStartFromEarliest();
                break;
            case "LATEST":
                kafkaConsumer.setStartFromLatest();
                break;
            case "TIMESTAMP":
//                kafkaConsumer.setStartFromTimestamp(config.getOffsetModeTimestamp());
                break;
            default:
                kafkaConsumer.setStartFromGroupOffsets();
                break;
        }

        if (enableCommitOnCheckpoint) {
            kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        } else {
            kafkaConsumer.setCommitOffsetsOnCheckpoints(false);
        }

    }

    @Test
    public void testKafkaSource() throws Exception {
        StreamExecutionEnvironment streamEnv = getStreamEnv();
        Properties  kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.110.78:9092");
//        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getSimpleName());

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("testSourceTopic", new SimpleStringSchema(), kafkaProps);
        setKafkaConsumer("earliest", true, kafkaConsumer);
        testConsumerKafkaToJson(streamEnv, kafkaConsumer);

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
        runFlinkKafka2KafkaDemo(env, "bdnode102:9092", "testSourceTopic", "testSinkTopic", kafkaProps);

    }



}
