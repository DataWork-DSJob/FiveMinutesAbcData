package flink.debug.pressure;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ParallismJsonStringSource extends RichParallelSourceFunction<JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallismJsonStringSource.class);

    private final Map<String, List<Object>> templates;
    private int count = 0;
    private boolean isRunning;

    private transient Map<String, List<Object>> runtimeDataTemplates;

    public ParallismJsonStringSource(Map<String, List<Object>> template) {
        this.templates = template;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Map<String, List<Object>> copyTemplates = new HashMap<>();
        if (null != templates && !templates.isEmpty()) {
            templates.forEach((k, v) -> {
                ArrayList<Object> copyValues = new ArrayList<>(v.size());
                for (Object obj: v) {
                    copyValues.add(obj);
                }
                copyTemplates.put(k, copyValues);

            });
        } else {
//            copyTemplates.put("Value1")
            for (int i = 0; i < 5; i++) {
                copyTemplates.put("Value_" + i, Arrays.asList("a", "b", "c"));
            }
        }
        this.runtimeDataTemplates = copyTemplates;
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<JSONObject> ctx) {
        RuntimeContext runtimeContext = getRuntimeContext();
        final int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
        int attemptNumber = runtimeContext.getAttemptNumber();
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
        String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();

        Map<String, List<Object>> dataTemplates = this.runtimeDataTemplates;
        Random random = new Random();
        boolean hasTemplateToAppend = dataTemplates.isEmpty() ? false : true;
        long lastPrint = System.nanoTime();
        long intervalRecordCount = 0;
        try {
            while (this.isRunning) {
                long nanoTime = System.nanoTime();
                JSONObject record = new JSONObject();
                record.put("time", nanoTime / 1000000);
                record.put("nanoTime", nanoTime);
                record.put("numberOfParallelSubtasks", numberOfParallelSubtasks);
                record.put("attemptNumber", attemptNumber);
                record.put("indexOfThisSubtask", indexOfThisSubtask);
                record.put("taskNameWithSubtasks", taskNameWithSubtasks);
                record.put("dataTemplates", dataTemplates);

                if (hasTemplateToAppend) {
                    for (Map.Entry<String, List<Object>> entry: dataTemplates.entrySet()) {
                        List<Object> list = entry.getValue();
                        record.put(entry.getKey(), list.get(random.nextInt(list.size())));
                    }
                }
                ctx.collect(record);
                ++intervalRecordCount;
                long delta = (nanoTime - lastPrint) / 1000000;
                if (delta > 1000 * 10) {
                    final long durationSeconds = (nanoTime - lastPrint) / 1000000000;
                    final long size = intervalRecordCount;
                    final long qps = size / durationSeconds;
//                    LOG.info("QpsPrint:  durationSeconds={} sec, recordSize={}, qps={}", durationSeconds, size, qps);
                    System.out.println(String.format("QpsPrint:  durationSeconds={ %d} sec, recordSize={%d}, qps={ %d }", durationSeconds, size, qps));
                    lastPrint = nanoTime;
                    intervalRecordCount = 0;
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void cancel() { }
}
