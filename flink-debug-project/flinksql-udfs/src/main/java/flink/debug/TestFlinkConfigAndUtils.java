package flink.debug;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.Test;

/**
 * @Author: jiaqing.he
 * @DateTime: 2022/10/27 18:14
 */
public class TestFlinkConfigAndUtils {

    @Test
    public void assignToKeyGroup() {
        int index = KeyGroupRangeAssignment.assignToKeyGroup("Key_3", 128);
        System.out.println("Key_3" + " -> " + index);
    }

}
