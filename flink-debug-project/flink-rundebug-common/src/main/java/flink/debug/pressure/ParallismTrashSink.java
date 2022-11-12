package flink.debug.pressure;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/11/5 0:06
 *  @param <IN>
 */

public class ParallismTrashSink<IN> extends RichSinkFunction<IN> {

    private long recordCount;
    private long lastCount;
    private long lastCountTime;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        recordCount = 0;
        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
        es.scheduleAtFixedRate(() -> {
            final long curRecordCount = recordCount;
            long curTime = System.currentTimeMillis();
            long delta = curRecordCount - lastCount;
            long duration =  curTime - lastCountTime;
            double qps = delta / (duration * 1000);

            System.err.println(String.format("SinkPring:  durationSeconds={ %d} sec, recordSize={%d}, qps={ %d }", duration / 1000, delta, qps));

            lastCount = curRecordCount;
            lastCountTime = curTime;

        },  0, 10,  TimeUnit.SECONDS);
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        value = null;
        ++recordCount;
    }


}
