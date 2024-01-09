package flink.debug.pressure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class SimpleSource<T> extends RichSourceFunction<T> {

    private final int maxBatch;
    private final List<T> template;

    private boolean isRunning;

    public SimpleSource(int maxBatch, T... data) {
        this.maxBatch = maxBatch;
        this.template = Arrays.stream(data).collect(Collectors.toList());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.isRunning = true;
    }

        @Override
    public void run(SourceContext<T> ctx) throws Exception {
        try {
            int count = 0;
            Random random = new Random();
            int size = template.size();
            while (this.isRunning) {
                T record = template.get(random.nextInt(size));
                ctx.collect(record);
                count ++;
                if (maxBatch > 0 && count >= maxBatch) {
                    break;
                }
                Thread.sleep(500 + random.nextInt(1000));
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
