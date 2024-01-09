package flink.apistudy.table.udf.percentv1;

import org.apache.flink.table.functions.AggregateFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @projectName: FiveMinutesAbcData
 * @className: PercentageCalculation
 * @description: flink.apistudy.table.udf.percentv1.PercentageCalculation
 * @author: jiaqing.he
 * @date: 2023/8/29 13:42
 * @version: 1.0
 */

//批量计算95line类似的数据
public class PercentageCalculation extends AggregateFunction<Object, PercentageAccumulator> {

    /**  */
    private static final long   serialVersionUID = 4009559061130131166L;
    private static final Logger LOG              = LoggerFactory
            .getLogger(PercentageCalculation.class);
    //private static BlockingQueue<PercentageAggregatorContainer> GLOBAL_QUEUE     = new LinkedBlockingQueue<PercentageAggregatorContainer>();

    @Override
    public PercentageAccumulator createAccumulator() {
        return new PercentageAccumulator();
    }

    public void accumulate(PercentageAccumulator accumulator, Object value) {
        accumulator.accumulate(value);
    }

    @Override
    public Object getValue(PercentageAccumulator accumulator) {
        return accumulator.getValue();
    }

    public void resetAccumulator(PercentageAccumulator acc) {
        acc = null;//help GC
    }

}
