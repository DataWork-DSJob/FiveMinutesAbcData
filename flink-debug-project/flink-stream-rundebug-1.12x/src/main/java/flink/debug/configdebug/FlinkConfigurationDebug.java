package flink.debug.configdebug;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

public class FlinkConfigurationDebug extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.110.79:9092";



    @Before
    public void setUp() {
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
    }


    @Test
    public void testReuseObjectByF112() throws Exception {
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
    public void testNetworkConfigByFlink112() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        pressureTest(env);
    }


    @Test
    public void testMiniClusterConfiguration() throws Exception {

        Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 6);
        runSimpleDemoJsonSource2WindowAgg2PrintWithEnv(configuration, -1, 1000);
    }




}
