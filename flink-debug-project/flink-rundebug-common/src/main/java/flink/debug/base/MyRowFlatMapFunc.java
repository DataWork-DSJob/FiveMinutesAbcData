package flink.debug.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyRowFlatMapFunc
 * @description: flink.debug.base.MyRowFlatMapFunc
 * @author: jiaqing.he
 * @date: 2023/4/11 10:14
 * @version: 1.0
 */
public class MyRowFlatMapFunc implements FlatMapFunction<String, Row> {
    @Override
    public void flatMap(String value, Collector<Row> out) throws Exception {
        // 25,keyA, 1676105055, 10.0
        //  0  1         2       3
        String[] split = value.split(",");
        Row row = null;
        if (null != split && split.length>3) {
            String id = split[0];
            String key = split[1];
            long timeSec = Long.parseLong(split[2].trim());
            double amount = Double.parseDouble(split[3].trim());
            String timeStr = new Date(timeSec * 1000).toLocaleString();
            // Row.of(id  0, key  1, timeSec*1000  2, amount  3, value  4);
            row = Row.of(id, key, timeSec * 1000, amount, value);
            out.collect(row);
        }
    }
}