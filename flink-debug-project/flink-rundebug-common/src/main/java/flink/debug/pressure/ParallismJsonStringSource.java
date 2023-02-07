package flink.debug.pressure;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
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
import java.util.concurrent.TimeUnit;

public class ParallismJsonStringSource extends RichParallelSourceFunction<JSONObject> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallismJsonStringSource.class);
    private static final String JSON_TEMPLATE = "{\n" +
            "\t\"timestamp\": 1669166469000,\n" +
            "\t\"message\": \"[{\\\"metric\\\":\\\"cpu_second\\\",\\\"prometheus\\\":\\\"cpu_second{instance=\\\\\\\"hadoop01\\\\\\\", app_id=\\\\\\\"app_202211260928\\\\\\\"} 2580.21\\\",\\\"val\\\":2580.21},{\\\"metric\\\":\\\"heap_memory\\\",\\\"prometheus\\\":\\\"heap_memory{instance=\\\\\\\"kafka6\\\\\\\", app_id=\\\\\\\"app_20221123933\\\\\\\"} 10485760\\\",\\\"val\\\":10485760}]\",\n" +
            "\t\"logHeader\": \"1669166386000|320|1|ZGBX|node1002\"\n" +
            "}\n";

    private final Map<String, List<Object>> templates;
    private final JSONObject fixJsonTemplate;

    private boolean isRunning;

    private transient Map<String, List<Object>> runtimeDataTemplates;

    public ParallismJsonStringSource(Map<String, List<Object>> template) {
        this.templates = template;
        this.fixJsonTemplate = JSON.parseObject(JSON_TEMPLATE);
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
        boolean reuseEnabled = runtimeContext.getExecutionConfig().isObjectReuseEnabled();
        final int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
        int attemptNumber = runtimeContext.getAttemptNumber();
        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
        String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();

        Map<String, List<Object>> dataTemplates = this.runtimeDataTemplates;
        Random random = new Random();
        boolean hasTemplateToAppend = dataTemplates.isEmpty() ? false : true;

        MetricRegistry registry = new MetricRegistry();
        Meter meter = registry.meter("kafka_vm_record_send");
        Slf4jReporter.forRegistry(registry).outputTo(LOG).build().start(5, TimeUnit.SECONDS);

        fixJsonTemplate.put("numberOfParallelSubtasks", numberOfParallelSubtasks);
        fixJsonTemplate.put("attemptNumber", attemptNumber);
        fixJsonTemplate.put("indexOfThisSubtask", indexOfThisSubtask);
        fixJsonTemplate.put("taskNameWithSubtasks", taskNameWithSubtasks);

        try {
            while (this.isRunning) {
                JSONObject record;
                if (reuseEnabled) {
                    record = new JSONObject();
                    record.putAll(fixJsonTemplate);
                } else {
                    record = fixJsonTemplate;
                }
                fixJsonTemplate.put("nanoTime", System.nanoTime());
                if (hasTemplateToAppend) {
                    for (Map.Entry<String, List<Object>> entry: dataTemplates.entrySet()) {
                        List<Object> list = entry.getValue();
                        record.put(entry.getKey(), list.get(random.nextInt(list.size())));
                    }
                }
                ctx.collect(record);
                meter.mark();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void cancel() { }
}
