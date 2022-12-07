package flink.debug.configdebug;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

public class FlinkConfigurationDebug extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.110.79:9092";


    @Before
    public void setUp(){
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
    }


    @Test
    public void testReuseObject() throws Exception {
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


    /**
     *  IDEA 要验证 Network, 设置JVM 参数: -Xmx1024M -Xms1024M
     *      假设 task.process.size=2G 2048M
     *       heap = 2048 - overhead ( 256 + 128) =
     * @throws Exception
     */
    @Test
    public void testNetwork() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        pressureTest(env);
    }




}
