package flink.apistudy.table.udf.percentv2;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @projectName: FiveMinutesAbcData
 * @className: WeightedAvg
 * @description: flink.apistudy.table.udf.WeightedAvg
 * @author: jiaqing.he
 * @date: 2023/3/25 15:59
 * @version: 1.0
 */
public class MyPercentileAggFunc extends AggregateFunction<Double, Tuple2<Long, Double>> {

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

    private static final int MAX_PERCENTILE = 100;
    private static final int MIN_PERCENTILE = 0;
    private double percentile(Double[] data, double percentile, int scale) {
        if (percentile > MAX_PERCENTILE || percentile < MIN_PERCENTILE) {
            throw new IllegalArgumentException("percentile master: 100 >= p > 0; 百分位必须小于100");
        }
        int len = data.length;
        Arrays.sort(data);
        double px = percentile / MAX_PERCENTILE * (len - 1);
        int i = (int) Math.floor(px);
        double g = px -i;
        double result;
        if (g == 0) {
            result = data[i];
        } else {
            double v1 = (1 - g) * data[i];
            double v2 = g * data[i + 1];
            result = v1 + v2;
        }
        return BigDecimal.valueOf(result).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    private List<Double> percentiles(Double[] valueArray, double[] percentiles, Integer scale) {
        List<Double> percentileList = new ArrayList(percentiles.length);
        for (int i = 0; i < percentiles.length; i++) {
            double conf = percentiles[i];
            percentileList.add(percentile(valueArray, conf, scale));
        }
        return percentileList;
    }

}
