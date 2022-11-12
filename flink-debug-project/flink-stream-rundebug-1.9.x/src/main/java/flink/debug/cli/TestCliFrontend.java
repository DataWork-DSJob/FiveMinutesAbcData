package flink.debug.cli;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.streaming.examples.socket.SocketWindowWordCount;
import org.junit.Test;

import java.util.StringJoiner;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/11/2 11:06
 */
public class TestCliFrontend {

    /**
     *  Window 系统 IDEA里提交 flink-yarn作业, 默认会因为 File.separator 不同导致在Window拼接的AM 的Classpath在 Linux 无法识别;
     *   - 社区早有人提出此Issues: https://issues.apache.org/jira/browse/FLINK-20973    但社区没改;
     *   - 这里根据issue中的加以, 用 YarnConfigOptions.APP_MASTER_VCORES 替换了相关的  File.separator
     *   - 开启测试前, 需要 mvn install 把 flink-yarn-winfix 打到本地仓库,否则汇报 localJarPath 为空的异常;
     *      - 参考源码 AbstractYarnClusterDescriptor.createDescriptor() 和 isReadyForDeployment() 出生成 localJarPath的源码;
     *
     */

    @Test
    public void test() {
        /**
         * --hostname bdnode124 --port 9000
         */
        SocketWindowWordCount socketWindowWordCount = new SocketWindowWordCount();
        System.setProperty("HADOOP_USER_NAME", "bigdata");
//
        printFlinkEnv();

        CliFrontend.main(new StringJoiner(" ")
                .add("run")
                .add("-m").add("yarn-cluster")
//                .add("-tm").add("2008") // Failed
                .add("-p").add("5")
                .add("-ys").add("3")
                .add("-yD").add("taskmanager.heap.size=2008m")
                .add("-yD").add("historyserver.archive.fs.refresh-interval=10003")
                .add("--detached")
                .add("-c").add(SocketWindowWordCount.class.getCanonicalName())
                .add("D:\\githubsrc\\bigdata-stream-debug\\" +
                        "flink-debug-project\\flink-stream-rundebug-1.9.x\\lib\\flink-examples-batch_2.11-1.9.3.jar")
                .add("--hostname").add("192.168.51.124")
                .add("--port").add("9000")
                .toString().split(" "));


    }

    private void printFlinkEnv() {

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

}
