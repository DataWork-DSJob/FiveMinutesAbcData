package flink.debug.runtime;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobmaster.DefaultSlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler;
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
        Class<AdaptiveScheduler> adaptiveSchedulerClass = AdaptiveScheduler.class;
        Class<DefaultSlotPoolServiceSchedulerFactory> factory = DefaultSlotPoolServiceSchedulerFactory.class;

        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 4);
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 6);
        runSimpleDemoJsonSource2WindowAgg2PrintWithEnv(configuration, -1, 1000);
    }


}
