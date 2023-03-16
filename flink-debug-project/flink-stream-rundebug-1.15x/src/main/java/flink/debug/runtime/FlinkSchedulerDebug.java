package flink.debug.runtime;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

public class FlinkSchedulerDebug extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.51.124:9092";

    @Before
    public void setUp() {
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
    }


    @Test
    public void testAdpateScheduler() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000L);
        env.setParallelism(2);
//        ExecutionConfig executionConfig = env.getConfig();
//        ExecutionConfig.GlobalJobParameters globalJobParameters = new ExecutionConfig.GlobalJobParameters();
//        globalJobParameters.toMap().put("scheduler-mode", "reactive");
//        executionConfig.setGlobalJobParameters(globalJobParameters);
//
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        env.configure(configuration);
        runSimpleDemoJsonSource2WindowAgg2Print(env, -1, 100000);
    }

}
