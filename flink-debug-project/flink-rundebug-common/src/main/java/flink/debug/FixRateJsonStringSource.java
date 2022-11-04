package flink.debug;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FixRateJsonStringSource implements SourceFunction<String> {
    private final int maxBatchCount;
    private final int recordRatePerSecond;
    private final Map<String, List<Object>> templates;
    private int count = 0;

    public FixRateJsonStringSource(int maxBatchCount, int recordRatePerSecond, Map<String, List<Object>> template) {
        this.maxBatchCount = maxBatchCount;
        this.recordRatePerSecond = recordRatePerSecond;
        this.templates = template;
    }

    @Override
    public void run(SourceContext<String> ctx) {
        try {
            while (maxBatchCount < 0 || count < maxBatchCount) {
                produceOnce(recordRatePerSecond, ctx);
                ++count;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    private void produceOnce(final double maxRecordsPerSec, SourceContext<String> ctx)  {
        Random random = new Random();
        final long batchStart = System.currentTimeMillis();
        final Map<String, Object> data = new HashMap<>();
        if (null == templates || templates.isEmpty()) {
            data.put("@message", "hello i am a test message");
            data.put("@tid", Thread.currentThread().getId());
        } else {
            templates.forEach((k, v) -> {
                if (null != v && !v.isEmpty()) {
                    Object value = v.get(random.nextInt(v.size()));
                    data.put(k, value);
                }
            });
        }
        data.put("@timestamp", DateTime.now().toString());
        data.put("batchCount",  String.valueOf(count));
        String json = JSON.toJSONString(data);
        ctx.collect(json);
        // 1000ms * sendSize > qps * duration
        // sendSize * 1000ms / qps > deltaMs
        // 实际耗时deltaMs < 该批数据量应该用时( sendSize*1000ms/qps), 要再等一等
        while ((System.currentTimeMillis() - batchStart) < 1 * 1000.0 / maxRecordsPerSec) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) { }
        }
    }

    @Override
    public void cancel() { }
}
