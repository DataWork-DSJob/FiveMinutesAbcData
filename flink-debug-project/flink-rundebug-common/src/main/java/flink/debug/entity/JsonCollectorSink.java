package flink.debug.entity;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.LinkedList;
import java.util.List;


/**
 * @projectName: FiveMinutesAbcData
 * @className: JsonCollectorSink
 * @description: flink.debug.entity.JsonCollectorSink
 * @author: jiaqing.he
 * @date: 2023/3/17 17:19
 * @version: 1.0
 */
public class JsonCollectorSink implements SinkFunction<JSONObject> {

    private List<JSONObject> jsonRecords;
    public JsonCollectorSink() {
        this.jsonRecords = new LinkedList<>();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        if (null != value) {
            this.jsonRecords.add(value);
        }
    }

    public List<JSONObject> getJsonRecords() {
        return jsonRecords;
    }

}
