package flink.apistudy.table.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @projectName: FiveMinutesAbcData
 * @className: HashFunction
 * @description: flink.apistudy.table.udf.HashFunction
 * @author: jiaqing.he
 * @date: 2023/3/25 14:22
 * @version: 1.0
 */
public class SplitAsArrayFunction extends ScalarFunction {

    public String[] eval(String inputStr, String delimiter) {
        if (null == inputStr || inputStr.isEmpty()) {
            return null;
        }
        String[] split = inputStr.split(delimiter);
        return split;
    }

}
