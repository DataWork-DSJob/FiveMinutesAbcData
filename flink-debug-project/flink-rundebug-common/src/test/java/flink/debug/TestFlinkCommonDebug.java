package flink.debug;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/10/27 16:00
 */
public class TestFlinkCommonDebug {

    @Ignore
    @Test
    public void test(){
        FlinkDebugCommon flinkDebugCommon = new FlinkDebugCommon();
        StreamExecutionEnvironment streamEnv = flinkDebugCommon.getStreamEnv();
        flinkDebugCommon.runFlinkKafkaConsumerDemo(streamEnv, "192.168.110.79:9092", "testSourceTopic", null);

    }

}
