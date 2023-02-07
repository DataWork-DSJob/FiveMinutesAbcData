package flink.debug.funcdebug;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;

public class FlinkWindowFunctionDebug extends FlinkDebugCommon {

    private String bootstrapServers = "192.168.110.79:9092";


    @Before
    public void setUp() {
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
    }


    @Test
    public void testFsStateByF112() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000 * 20, CheckpointingMode.EXACTLY_ONCE);

        Path dirPath = getOrCreateDirFromUserDir("checkpoint-fs");
        String backendPath = dirPath.toUri().toString();
        env.setStateBackend(new FsStateBackend(backendPath));

        pressureTest(env);
    }



}
