package flink.debug.utils;

import org.apache.flink.types.Row;

/**
 * @projectName: FiveMinutesAbcData
 * @className: RowUtils
 * @description: flink.debug.utils.RowUtils
 * @author: jiaqing.he
 * @date: 2023/3/22 12:51
 * @version: 1.0
 */
public class RowUtils {

    public static <T> T getOrNull(Row row, int index) {
        Object fieldValue = row.getField(index);
        if (null != fieldValue) {
           return  (T) fieldValue;
        }
        return null;
    }

    public static <T> T getOrDefault(Row row, int index, T defaultValue) {
        Object fieldValue = row.getField(index);
        if (null != fieldValue) {
            return  (T) fieldValue;
        }
        return defaultValue;
    }



}
