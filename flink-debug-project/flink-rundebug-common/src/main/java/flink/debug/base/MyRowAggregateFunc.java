package flink.debug.base;

import flink.debug.utils.RowUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyRowAggregateFunc
 * @description: flink.debug.base.MyRowAggregateFunc
 * @author: jiaqing.he
 * @date: 2023/4/11 10:17
 * @version: 1.0
 */
public class MyRowAggregateFunc implements AggregateFunction<Tuple2<String, Row>, Row, Row> {
    private final Logger LOG = LoggerFactory.getLogger(MyRowAggregateFunc.class);
    private final int rowSize = 10;
    @Override
    public Row createAccumulator() {
        return new Row(rowSize);
    }

    @Override
    public Row add(Tuple2<String, Row> value, Row accumulator) {
        Long count = RowUtils.getOrDefault(accumulator, 0, 0L);
        accumulator.setField(0, ++count);
        // Row.of(id  0, key  1, timeSec*1000  2, amount  3, value  4);
        double sum = RowUtils.getOrDefault(accumulator, 1, 0D)
                + RowUtils.getOrDefault( value.f1, 3, 0D);
        accumulator.setField(1, sum);

        List<String> originDataArray = RowUtils.getOrDefault(accumulator, 2, new LinkedList<String>());
        originDataArray.add(RowUtils.getOrDefault(value.f1, 4, "0L"));
        accumulator.setField(2, originDataArray);
        // accumulator: Row [ 0 count, 1 sum, 2 originDataArray ]

        String dataStr = value.f1.getField(4).toString();
        LOG.debug("WinAdd 窗口添加({}), key={}, row={}, accumulator={} ", dataStr, value.f0, value.f1, accumulator);
        return accumulator;
    }

    @Override
    public Row getResult(Row accumulator) {
        return accumulator;
    }

    @Override
    public Row merge(Row a, Row b) {
        long count = RowUtils.getOrDefault(a, 0, 0L) + RowUtils.getOrDefault(a, 0, 0L);
        a.setField(0, count);

        double sum = RowUtils.getOrDefault(a, 1, 0D) + RowUtils.getOrDefault(a, 1, 0D);
        a.setField(1, sum);

        List<String> aList = RowUtils.getOrDefault(a, 2, Collections.emptyList());
        List<String> bList = RowUtils.getOrDefault(b, 2, Collections.emptyList());
        List<String> originDataArray = new ArrayList<>(aList.size() + bList.size());
        originDataArray.addAll(aList);
        originDataArray.addAll(bList);
        a.setField(2, originDataArray);
        return a;
    }
}
