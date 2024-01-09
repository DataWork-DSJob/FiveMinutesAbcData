package flink.apistudy.table.udf.percentv2;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyPercentileAggFuncV2
 * @description: flink.apistudy.table.udf.percentv2.MyPercentileAggFuncV2
 * @author: jiaqing.he
 * @date: 2023/8/29 14:10
 * @version: 1.0
 */
@FunctionHint(output = @DataTypeHint("ARRAY<DOUBLE>"))
public class MultiPercentileAggFunc extends AggregateFunction<Double[], List<Double>> {

    @Override
    public Double[] getValue(List<Double> accumulator) {
        if (null == accumulator || accumulator.isEmpty()) {
            return null;
        }
        Double[] data = accumulator.toArray(new Double[accumulator.size()]);
        double ret50 = percentile(data, 50, 3);
        double ret75 = percentile(data, 75, 3);
        double ret90 = percentile(data, 90, 3);
        double ret95 = percentile(data, 95, 3);
        double ret99 = percentile(data, 99, 3);
        return new Double[]{ret50, ret75, ret90, ret95, ret99};
    }

    @Override
    public List<Double> createAccumulator() {
        return new LinkedList<>();
    }

    public void merge(List<Double> acc, Iterable<List<Double>> it) {
        Iterator<List<Double>> iter = it.iterator();
        while (iter.hasNext()) {
            List<Double> ele = iter.next();
            acc.addAll(ele);
        }
    }

    public void resetAccumulator(List<Double> acc) {
        acc.clear();
    }

    public void accumulate(List<Double> acc, Double value) {
        if (null != value) {
            acc.add(value);
        }
    }

    public void accumulate(List<Double> acc, Integer value) {
        if (null != value) {
            acc.add(value.doubleValue());
        }
    }

    public void accumulate(List<Double> acc, Long value) {
        if (null != value) {
            acc.add(value.doubleValue());
        }
    }

    public void accumulate(List<Double> acc, Float value) {
        if (null != value) {
            acc.add(value.doubleValue());
        }
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
