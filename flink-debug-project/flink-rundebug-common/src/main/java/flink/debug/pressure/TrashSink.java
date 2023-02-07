package flink.debug.pressure;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.TimeUnit;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/11/5 0:06
 *  @param <IN>
 */

public class TrashSink<IN> extends RichSinkFunction<IN> {

    private boolean enableSinkPrint;
    private PrintSinkOutputWriter<String> writer;
    public TrashSink() {
        this(false);
    }

    public TrashSink(boolean enableSinkPrint) {
        this.enableSinkPrint = enableSinkPrint;
    }


    private transient Meter qpsMeter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        MetricRegistry registry = new MetricRegistry();
        this.qpsMeter = registry.meter("trash_sink_qps_subtask-" + indexOfThisSubtask);
        ConsoleReporter.forRegistry(registry).build().start(5, TimeUnit.SECONDS );

        if (this.enableSinkPrint) {
            this.writer = new PrintSinkOutputWriter<>("printTrash", false);
            RuntimeContext runtimeContext = getRuntimeContext();
            int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
            this.writer.open(indexOfThisSubtask, numberOfParallelSubtasks);
        }

    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        this.qpsMeter.mark();
        if (this.enableSinkPrint) {
            writer.write(value.toString());
        }
    }


}
