package flink.debug.base;

import flink.debug.utils.RowUtils;
import flink.debug.utils.TimeHelper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyRowProcessFunc
 * @description: flink.debug.base.MyRowProcessFunc
 * @author: jiaqing.he
 * @date: 2023/4/11 10:19
 * @version: 1.0
 */
public class MyRowProcessFunc extends ProcessWindowFunction<Row, Row, String, TimeWindow> {

    @Override
    public void process(String key, ProcessWindowFunction<Row, Row, String, TimeWindow>.Context context,
                        Iterable<Row> elements, Collector<Row> out) throws Exception {

        long count = 0L;
        double sum = 0D;
        List<String> originDataArray = new LinkedList<>();
        Iterator<Row> it = elements.iterator();
        while (it.hasNext()) {
            Row acc = it.next();
            count += RowUtils.getOrDefault(acc, 0, 0L);
            sum += RowUtils.getOrDefault(acc, 1, 0D);
            List<String> arr = RowUtils.getOrDefault(acc, 2, Collections.emptyList());
            if (null != acc) {
                originDataArray.addAll(arr);
            }
        }

        String windowStart = TimeHelper.Format.formatAsCommStr(context.window().getStart());
        String windowEnd = TimeHelper.Format.formatAsCommStr(context.window().getEnd());
        String currentWatermark = TimeHelper.Format.formatAsCommStr(context.currentWatermark());

        Row result = Row.of(key, windowStart, count, sum, windowEnd, currentWatermark, originDataArray);
        out.collect(result);
//            LOG.debug("WinAdd 窗口添加({}), key={}, row={}, accumulator={} ", dataStr, value.f0, value.f1, accumulator);
    }

}