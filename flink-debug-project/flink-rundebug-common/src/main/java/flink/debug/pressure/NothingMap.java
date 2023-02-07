package flink.debug.pressure;

import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @projectName: FiveMinutesAbcData
 * @className: NothingMap
 * @description: flink.debug.pressure.NothingMap
 * @author: jiaqing.he
 * @date: 2022/12/20 15:12
 * @version: 1.0
 */
public class NothingMap<T> extends RichMapFunction<T, T> {

    @Override
    public T map(T value) throws Exception {
        return value;
    }

}
