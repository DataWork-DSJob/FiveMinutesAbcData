package flink.debug.configdebug;


import flink.debug.FlinkDebugCommon;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;

public class FlinkStateCheckpointDebug extends FlinkDebugCommon {

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



    @Test
    public void testRocksdbStateByF112() throws Exception {
        Thread.sleep(1000 * 60 );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000 * 20, CheckpointingMode.EXACTLY_ONCE);

        Path dirPath = getOrCreateDirFromUserDir("checkpoint-rocksdb");
        String backendPath = dirPath.toUri().toString();
        RocksDBStateBackend backend =  new RocksDBStateBackend(backendPath);
        backend.setNumberOfTransferThreads(3);
//        RocksDBStateBackend backend =  new RocksDBStateBackend(backendPath, true);
//        backend.setPriorityQueueStateType(RocksDBStateBackend.PriorityQueueStateType.HEAP);
        env.setStateBackend(backend);

        pressureTest(env);
    }






}
