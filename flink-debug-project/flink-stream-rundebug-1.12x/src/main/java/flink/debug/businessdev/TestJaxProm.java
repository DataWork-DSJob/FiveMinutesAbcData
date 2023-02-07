package flink.debug.businessdev;

import com.eoi.jax.metrics.kafka.PromKafkaReporter;
import org.junit.Test;

/**
 * @projectName: FiveMinutesAbcData
 * @className: TestJaxProm
 * @description: flink.debug.businessdev.TestJaxProm
 * @author: jiaqing.he
 * @date: 2023/1/3 18:35
 * @version: 1.0
 */
public class TestJaxProm {


    @Test
    public void testJax() {
        PromKafkaReporter promKafkaReporter = new PromKafkaReporter();
        promKafkaReporter.report();

    }
}
