package flink.debug.tableapi;

import flink.debug.FlinkSqlTableCommon;
import org.junit.Test;

/**
 * @projectName: FiveMinutesAbcData
 * @className: FlinkTableExample
 * @description: flink.debug.tableapi.FlinkTableExample
 * @author: jiaqing.he
 * @date: 2023/7/27 10:26
 * @version: 1.0
 */
public class FlinkTableExample extends FlinkSqlTableCommon {

    @Test
    public void sqlBuiltInFunctionsByF115() throws Exception {
        runSqlBuiltInFunctions(null, null);
    }


    @Test
    public void simpleTableDemoByF115() throws Exception {
        runSimpleTableDemoDatagen2WindowAgg2Print(null, null);
    }


}
