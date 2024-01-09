package flink.apistudy.table.udf;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.util.Iterator;

/**
 * @projectName: FiveMinutesAbcData
 * @className: WeightedAvg
 * @description: flink.apistudy.table.udf.WeightedAvg
 * @author: jiaqing.he
 * @date: 2023/3/25 15:59
 * @version: 1.0
 */
//@FunctionHint(output = @DataTypeHint("ROW<index INT, onRight BOOLEAN, word STRING, length INT>"))
public class Top3TableAggFunc extends TableAggregateFunction<Tuple3<Integer, Integer, Double>, Top3TableAggFunc.Top2Acc> {

    public class Top2Acc {
        public Integer first;
        public Integer second;
    }

    @Override
    public Top2Acc createAccumulator() {
        Top2Acc acc = new Top2Acc();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Acc acc, Integer value) {
        if (null != value) {
            if (value > acc.first) {
                acc.first = value;
                acc.second = acc.first;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }
    }

    public void merge(Top2Acc acc, Iterable<Top2Acc> it) {
        Iterator<Top2Acc> iter = it.iterator();
        while (iter.hasNext()) {
            Top2Acc next = iter.next();
            accumulate(acc, next.first);
            accumulate(acc, next.second);
        }
    }

    public void resetAccumulator(Top2Acc acc) {
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
    }


    public void emitValue(Top2Acc acc, org.apache.flink.util.Collector<Tuple3<Integer, Integer, Double>> out) {
        Integer first = acc.first;
        long sum = acc.first + acc.second;
        if (acc.first != Integer.MIN_VALUE) {
            // rank, value,
            out.collect(Tuple3.of(1, acc.first, acc.first / sum * 100.0D));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple3.of(2, acc.second, acc.second / sum * 100.0D));
        }
    }


}
