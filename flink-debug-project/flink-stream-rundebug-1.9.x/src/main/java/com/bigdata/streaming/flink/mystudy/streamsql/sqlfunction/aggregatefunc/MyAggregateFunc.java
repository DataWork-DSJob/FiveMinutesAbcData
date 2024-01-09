package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.aggregatefunc;


import org.apache.flink.table.functions.AggregateFunction;

public class MyAggregateFunc extends AggregateFunction {


    @Override
    public Object getValue(Object accumulator) {
        return null;
    }

    @Override
    public Object createAccumulator() {
        return null;
    }

}
