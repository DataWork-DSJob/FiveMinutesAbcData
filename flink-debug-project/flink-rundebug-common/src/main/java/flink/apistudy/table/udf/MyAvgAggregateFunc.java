package flink.apistudy.table.udf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * @projectName: FiveMinutesAbcData
 * @className: WeightedAvg
 * @description: flink.apistudy.table.udf.WeightedAvg
 * @author: jiaqing.he
 * @date: 2023/3/25 15:59
 * @version: 1.0
 */
public class MyAvgAggregateFunc extends AggregateFunction<Double, Tuple2<Long, Double>> {

    @Override
    public Tuple2<Long, Double> createAccumulator() {
        return new Tuple2<>(0L, 0D);
    }

    @Override
    public Double getValue(Tuple2<Long, Double> accumulator) {
        if (null == accumulator || null == accumulator.f0) {
            return 0D;
        }
        return accumulator.f1 / accumulator.f0;
    }

    public void accumulate(Tuple2<Long, Double> acc, Double value) {
        acc.f1 += value;
        acc.f0 += 1;
    }

    public void retract(Tuple2<Long, Double> acc, Double value) {
        acc.f1 -= value;
        acc.f0 -= 1;
    }

    public void merge(Tuple2<Long, Double> acc, Iterable<Tuple2<Long, Double>> it) {
        Iterator<Tuple2<Long, Double>> iter = it.iterator();
        while (iter.hasNext()) {
            Tuple2<Long, Double> ele = iter.next();
            acc.f0 += ele.f0;
            acc.f1 = ele.f1;
        }
    }

    public void resetAccumulator(Tuple2<Long, Double> acc) {
        acc.f0 = 0L;
        acc.f1 = 0D;
    }

    @Override
    public TypeInformation<Double> getResultType() {
        return TypeInformation.of(new TypeHint<Double>(){});
    }

    @Override
    public TypeInformation<Tuple2<Long, Double>> getAccumulatorType() {
        return TypeInformation.of(new TypeHint<Tuple2<Long, Double>>(){});
    }

}
