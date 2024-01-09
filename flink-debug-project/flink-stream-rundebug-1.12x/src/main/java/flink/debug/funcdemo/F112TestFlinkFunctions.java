package flink.debug.funcdemo;


import org.junit.Test;

public class F112TestFlinkFunctions extends FlinkFunctionsDemo {


    @Test
    public void testWatermarkByF112() throws Exception {
        testWatermark();
    }

//    @Data
    class WaterSensor {
        String id;
        long ts;
        Double vc;
        String originData;
        public WaterSensor(String id, long ts, Double vc, String originData) {
            this.id = id;
            this.ts = ts;
            this.vc = vc;
            this.originData = originData;
        }
    }

    @Test
    public void testSocketWindowAggByF112() throws Exception {
        testSocketWindowAgg();

    }




}
