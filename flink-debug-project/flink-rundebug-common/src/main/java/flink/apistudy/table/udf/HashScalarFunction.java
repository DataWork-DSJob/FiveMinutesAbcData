package flink.apistudy.table.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @projectName: FiveMinutesAbcData
 * @className: HashFunction
 * @description: flink.apistudy.table.udf.HashFunction
 * @author: jiaqing.he
 * @date: 2023/3/25 14:22
 * @version: 1.0
 */
public class HashScalarFunction extends ScalarFunction {

    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return o.hashCode();
    }


}
