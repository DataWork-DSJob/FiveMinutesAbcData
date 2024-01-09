package flink.debug.base;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public class MyStringSource implements SourceFunction<String> {
    private final int maxBatchCount;
    private final List<String> lines;
    private boolean isRunning = true;

    public MyStringSource(Integer maxBatchCount, String... fixLines) {
        this.maxBatchCount = null == maxBatchCount ? 1 : maxBatchCount;
        this.lines = Arrays.asList(fixLines);
    }

    @Override
    public void run(SourceContext<String> ctx) {
        RateLimiterCopy rateLimiter = RateLimiterCopy.create(0.5);
        List<String> lines = this.lines;
        int count = 0;
        try {
            while (isRunning && (maxBatchCount < 0 || count < maxBatchCount)) {
                for (String line: lines) {
                    ctx.collect(line);
                    rateLimiter.acquire();
                    count ++;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
