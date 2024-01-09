package flink.debug.cep;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestCepDemo {

    private String bootstrapServers = "192.168.51.124:9092";
    
    @Test
    public void testCepFlinkCase() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> input =
                env.fromElements(
                        new Event(1, "barfoo", 1.0),
                        new Event(2, "start", 2.0),
                        new Event(3, "foobar", 3.0),
                        new SubEvent(4, "foo", 4.0, 1.0),
                        new Event(5, "middle", 5.0),
                        new SubEvent(6, "middle", 6.0, 2.0),
                        new SubEvent(7, "bar", 3.0, 3.0),
                        new Event(42, "42", 42.0),
                        new Event(8, "end", 1.0));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("start");
                                    }
                                })
                        .followedByAny("middle")
                        .subtype(SubEvent.class)
                        .where(
                                new SimpleCondition<SubEvent>() {

                                    @Override
                                    public boolean filter(SubEvent value) throws Exception {
                                        return value.getName().equals("middle");
                                    }
                                })
                        .followedByAny("end")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("end");
                                    }
                                });

        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inProcessingTime()
                        .flatSelect(
                                (p, o) -> {
                                    StringBuilder builder = new StringBuilder();

                                    builder.append(p.get("start").get(0).getId())
                                            .append(",")
                                            .append(p.get("middle").get(0).getId())
                                            .append(",")
                                            .append(p.get("end").get(0).getId());

                                    o.collect(builder.toString());
                                },
                                Types.STRING);

        List<String> resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);


        Assert.assertEquals(Arrays.asList("2,6,8"), resultList);

    }

    @Test
    public void testFlinkCepV2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> input =
                env.fromElements(
                        new Event(1, "barfoo", 1.0),
                        new Event(2, "start", 2.0),
                        new Event(3, "foobar", 3.0),
                        new SubEvent(4, "foo", 4.0, 1.0),
                        new Event(5, "middle", 5.0),
                        new SubEvent(6, "middle", 6.0, 2.0),
                        new SubEvent(7, "bar", 3.0, 3.0),
                        new Event(42, "42", 42.0),
                        new Event(8, "end", 1.0));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
//                                        return value.getName().equals("start");
                                        return value.getId() > 2;
                                    }
                                })
                .next("middle")
                        .subtype(SubEvent.class)
                        .where(new SimpleCondition<SubEvent>() {
                            @Override
                            public boolean filter(SubEvent value) throws Exception {
                                return value.getVolume() >= 2.0;
                            }
                        })
                .followedBy("end")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return value.getName().equals("end");
                            }
                        })
                ;

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        SingleOutputStreamOperator<Map> result = patternStream
                .inProcessingTime()
                .process(new PatternProcessFunction<Event, Map>() {
                    @Override
                    public void processMatch(Map<String, List<Event>> map, Context context, Collector<Map> out) throws Exception {
                        HashMap<String, Object> record = new LinkedHashMap<>();
                        record.put("eventSize", map.size());
                        record.put("keySet", map.keySet());
                        record.put("record", map);
                        out.collect(record);
                    }
                });

//        DataStreamSink<Map> print = result.print();

        DataStream<String> result2 = patternStream
                        .inProcessingTime()
                        .flatSelect(
                                (p, o) -> {

                                    Map<String, Object> record = new LinkedHashMap<>();
                                    record.put("eventSize", p.size());
                                    record.put("keySet", p.keySet());
                                    record.put("record", p);
                                    o.collect(record.toString());

//                                    StringBuilder builder = new StringBuilder();
//                                    builder.append(p.get("start").get(0).getId())
//                                            .append(",")
//                                            .append(p.get("middle").get(0).getId())
//                                            .append(",")
//                                            .append(p.get("end").get(0).getId());
//                                    o.collect(builder.toString());
                                },
                                Types.STRING);

        List resultList = new ArrayList<>();

        DataStreamUtils.collect(result).forEachRemaining(resultList::add);


        Assert.assertEquals(Arrays.asList("2,6,8"), resultList);

    }


}
