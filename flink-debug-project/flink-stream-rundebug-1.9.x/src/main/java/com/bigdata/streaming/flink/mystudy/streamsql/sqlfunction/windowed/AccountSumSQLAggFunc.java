package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed;

import org.apache.flink.table.functions.AggregateFunction;


public class AccountSumSQLAggFunc extends AggregateFunction<Double, AccountSumSQLAggFunc.AccSum> {


    public void accumulate(int num,double price){

    }


    @Override
    public Double getValue(AccSum accumulator) {
        return null;
    }

    @Override
    public AccSum createAccumulator() {
        return null;
    }

    public static class AccSum{
        double sum;
    }


}


