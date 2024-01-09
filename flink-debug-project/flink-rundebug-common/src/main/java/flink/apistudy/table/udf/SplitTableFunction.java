package flink.apistudy.table.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @projectName: FiveMinutesAbcData
 * @className: SplitFunction
 * @description: flink.apistudy.table.udf.SplitFunction
 * @author: jiaqing.he
 * @date: 2023/3/25 14:49
 * @version: 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<index INT, onRight BOOLEAN, word STRING, length INT>"))
public class SplitTableFunction extends TableFunction<Row> {

    public void eval(String str) {
        String[] split = str.split(" ");
        int length = split.length;
        for (int i = 0; i < split.length; i++) {
            int len = split[i].length();
            long procTime = System.currentTimeMillis();
            boolean right = i * 2 > length;
            Row row = Row.of(i, right, split[i], len);
            collect(row);
        }
    }


}
