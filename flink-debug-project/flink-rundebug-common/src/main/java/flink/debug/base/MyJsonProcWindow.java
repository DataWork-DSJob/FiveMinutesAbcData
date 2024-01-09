package flink.debug.base;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @projectName: FiveMinutesAbcData
 * @className: MyJsonProcWindow
 * @description: flink.debug.base.MyJsonProcWindow
 * @author: jiaqing.he
 * @date: 2023/4/11 10:07
 * @version: 1.0
 */
public class MyJsonProcWindow extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
        JSONObject json = new JSONObject();
        json.put("key", key);
        Iterator<JSONObject> it = elements.iterator();
        int count = 0;
        while (it.hasNext()) {
            JSONObject data = it.next();
            count++;
        }

        json.put("count", count);
        out.collect(json);

        Thread.sleep(500);
    }
}
