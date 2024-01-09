package flink.debug.base;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Random;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyKeySelector
 * @description: flink.debug.base.MyKeySelector
 * @author: jiaqing.he
 * @date: 2023/4/11 10:06
 * @version: 1.0
 */
public class MyKeySelector implements KeySelector<JSONObject, String> {
    private Random random = new Random();
    private int randomSuffixNum = 5;
    @Override
    public String getKey(JSONObject value) throws Exception {
        String groupKey = "groupKey_";
        try {
            String valueString = value.getString("groupKey");
            if (null == valueString) {
                valueString = "groupKey_" + random.nextInt(randomSuffixNum);
            }
            groupKey = valueString;
        } catch (Exception e) {
            groupKey = "groupKey_" + random.nextInt(randomSuffixNum);
        }
        return groupKey;
    }
}