package flink.debug.runtime;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import flink.debug.FlinkDebugCommon;
import flink.debug.pressure.TrashSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

public class FlinkObjectReuseDebug extends FlinkDebugCommon {

    public static final JSONObject JSON_OBJECT = new JSONObject();

    @Before
    public void setUp() {
        setClasspathResourceAsJvmEnv("FLINK_CONF_DIR", "flinkConfDir");
        setClasspathResourceAsJvmEnv("HADOOP_CONF_DIR", "hadoopConfDir");
        printFlinkEnv();
    }


    @Test
    public void testDagChainMultiOutputObjReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        JSONObject value = new JSONObject();
        value.put("origin", value.hashCode());
        SingleOutputStreamOperator<JSONObject> process = env
                .fromElements(value)
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {

                        return value;
                    }
                })
                .name("二, Only Header Map, 头, 仅一个")
                ;

        // 主逻辑里, 值改为 A
        process
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("flag", "true");
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("main_process");
                        value.put("tags", tags);
                        value.put("tag_main_process", value.hashCode());
                        return value;
                    }
                })
                .name("三, Main Map, 主map逻辑")
                .addSink(new TrashSink<>(true))
                .name("四, Main Sink,主输出")
        ;

        // Multi Output Map, 第二个 并列map
        process
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("flag", "false");
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("side_tag");
                        value.put("tags", tags);
                        value.put("tag_side", value.hashCode());
                        return value;
                    }
                })
                .name("3.2, Multi Map, 多输出-map算子")
                .addSink(new TrashSink<>(true))
                .name("4.2 Multi Sink, 多输出-输出算子 ")
        ;

        env.getConfig().enableObjectReuse();
//        env.getConfig().disableObjectReuse();


        env.execute(this.getClass().getSimpleName());

    }


    @Test
    public void testDagChainSideOutputObjReuse() throws Exception {
        OutputTag<JSONObject> outSide = new OutputTag<JSONObject>("outSide", TypeInformation.of(JSONObject.class));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        JSONObject value = new JSONObject();
        value.put("origin", value.hashCode());
        SingleOutputStreamOperator<JSONObject> process = env
                .fromElements(value)
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        out.collect(value);
                        ctx.output(outSide, value);
                    }
                })
                .name("二, Process, collect-sideOutTag")
                ;

        // 主逻辑里, 值改为 A
        process
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("flag", "true");
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("main_process");
                        value.put("tags", tags);
                        value.put("tag_main_process", value.hashCode());
                        return value;
                    }
                })
                .name("三, Main Map, 主map逻辑")
                .addSink(new TrashSink<>(true))
                .name("四, Main Sink,主输出")
        ;

        // 侧边逻辑里, 值改为2
        process
                .getSideOutput(outSide)
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("flag", "false");
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("side_tag");
                        value.put("tags", tags);
                        value.put("tag_side", value.hashCode());
                        return value;
                    }
                })
                .name("3.2, Side Map, 测流-map算子")
                .addSink(new TrashSink<>(true))
                .name("4.2 Side Sink, 测流输出 ")
        ;

        env.getConfig().enableObjectReuse();
//        env.getConfig().disableObjectReuse();


        env.execute();

    }

    @Test
    public void testAutoOptimizeByObjReuse() throws Exception {
        OutputTag<JSONObject> outSide = new OutputTag<JSONObject>("outSide", TypeInformation.of(JSONObject.class));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        JSONObject value = new JSONObject();
        value.put("origin", value.hashCode());
        SingleOutputStreamOperator<JSONObject> process = env
                .fromElements(value)
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        out.collect(value);
                        ctx.output(outSide, value);
                    }
                })
                .name("二, Process, collect-sideOutTag")
                ;

        // 主逻辑里, 值改为 A
        process
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("num", 1);
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("main_process");
                        value.put("tags", tags);
                        value.put("tag_main_process", value.hashCode());
                        return value;
                    }
                })
                .name("三, Main Map, 主map逻辑")
                .addSink(new TrashSink<>(true))
                .name("四, Main Sink,主输出")
                ;


        // 多输出逻辑 2
        process
                .getSideOutput(outSide)
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("num", 2);
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("multi_output_2");
                        value.put("tags", tags);
                        value.put("hash_tag_multiOut_2", value.hashCode());
                        return value;
                    }
                })
                .name("3.2, Multi Map, 多输出-map算子")
                .addSink(new TrashSink<>(true))
                .name("4.2 Multi Sink, 多输出-输出算子 ")
        ;

        // 侧边逻辑里, 值改为3
        process
                .getSideOutput(outSide)
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        value.put("num", 3);
                        JSONArray tags = value.getJSONArray("tags");
                        if (null == tags) {
                            tags = new JSONArray();
                        }
                        tags.add("side_output_3");
                        value.put("tags", tags);
                        value.put("hash_tag_sideOut3", value.hashCode());
                        return value;
                    }
                })
                .name("3.3, Side Map, 测流-map算子")
                .addSink(new TrashSink<>(true))
                .name("4.3 Side Sink, 测流-输出算子 ")
        ;


        boolean hasMultiOutput = false;
        // 判断是否有多输出(OutEdge)情况, 如果某个算子下游输出2个以上, 则 hasMultiOutput = true,需禁用 对象重用(ObjectReuse);
        // 这里 clearTransformations必需填false, 否则后面执行会报错;
        StreamGraph streamGraph = env.getStreamGraph( false);
        Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
        for (StreamNode streamNode: streamNodes) {
            if (streamNode.getOutEdges().size() > 1) {
                hasMultiOutput = true;
            }
        }
        if (hasMultiOutput) {
            env.getConfig().disableObjectReuse();
        } else {
            // AUTO_OPTIMIZE 模式下, 默认开启 ObjectReuse 优化性能
            env.getConfig().enableObjectReuse();
        }

        env.execute(this.getClass().getSimpleName());

    }




}
