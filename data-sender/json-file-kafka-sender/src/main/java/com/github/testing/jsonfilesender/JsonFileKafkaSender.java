package com.github.testing.jsonfilesender;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/9/16 11:24
 */
public class JsonFileKafkaSender {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFileKafkaSender.class);

    public static void main(String[] args) {
        Argument argument = new Argument();
        JCommander jc = JCommander.newBuilder().addObject(argument).build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            e.printStackTrace();
            jc.usage();
            System.exit(1);
        }

        JsonFileKafkaSender instance = new JsonFileKafkaSender();
        try {
            instance.run(argument);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private List<KafkaSenderRunnable> calcKafkaRunners(Argument argument) {
        FileJsonCreator strRecordCreator = new FileJsonCreator();
        strRecordCreator.init(argument);

        int recordsPerSecond = argument.getRecordsPerSecond();
        int maxRatePerThread = argument.getMaxRatePerThread();
        List<KafkaSenderRunnable> runnables = new LinkedList();
        if (recordsPerSecond > maxRatePerThread) {
            int leave = recordsPerSecond;
            int runnerCount = 0;
            while (leave > 0) {
                runnerCount++;
                int reduce = leave >= maxRatePerThread ? maxRatePerThread : leave;
                runnables.add(new KafkaSenderRunnable(argument, strRecordCreator, reduce, runnerCount));
                leave -= reduce;
            }
        } else {
            runnables.add(new KafkaSenderRunnable(argument, strRecordCreator, recordsPerSecond, 1));
        }
        return runnables;
    }

    private void run(Argument argument) throws Exception {
        List<KafkaSenderRunnable> runnables = calcKafkaRunners(argument);
        LOG.info("=========== KafkaSenderRunnable info ==================");
        LOG.info("    enableNotLimitRate 是否限速 : " + argument.isEnableNotLimitRate()
                + (argument.isEnableNotLimitRate() ? "不限速, 各线程尽量发" : "限定速率"));
        LOG.info("    RecordsPerSecond 速率      : " + argument.getRecordsPerSecond());
        LOG.info("    MaxRatePerThread 单线程max : " + argument.getMaxRatePerThread());
        LOG.info("    BootstrapServices         : " + argument.getBootstrapServices());
        LOG.info("    Topic               -     : " + argument.getTopic());
        LOG.info("    runner.size               : " + runnables.size());
        LOG.info("    runners                   : " + runnables);
        LOG.info("=========== info end ==================");

        if (1 == runnables.size()) {
            runnables.get(0).run();
        } else {
            int poolSize = runnables.size();
            ExecutorService executor = new ThreadPoolExecutor(poolSize, poolSize, 0L,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
            for (KafkaSenderRunnable spliter: runnables) {
                executor.submit(spliter);
                Thread.sleep(2000);
            }
            while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                LOG.info("wait");
            }
        }
        LOG.info("All Finish and Exit");
        System.exit(0);

    }

}
