package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed.simpletest;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Collections;
import java.util.List;

public class LastValueIncrementAggFunc extends AggregateFunction<Integer,LastValueAcc<Integer>> implements ListCheckpointed<Integer> {

    ListState<Integer> lastState;

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
        Iterable<Integer> eles = lastState.get();
        int lastValue = 0;
        for(Integer ele:eles){
            lastValue = ele;
        }
        return Collections.singletonList(lastValue);
    }


    @Override
    public void restoreState(List<Integer> state) throws Exception {

    }



    @Override
    public LastValueAcc<Integer> createAccumulator() {
        return new LastValueAcc(0);
    }

    // 在Sql中 调用该类,会触发 this.accumulate(arg1,arg2);
    public void accumulate(LastValueAcc<Integer> acc, long time,int approduction){

        acc.setLastValue(approduction);
    }

    // 在sql中, 当事务失败需要回滚, 则把刚 把结果回滚;
    public void retract(LastValueAcc<Integer> acc,int arg1){
        Integer last = acc.getLastValue();
        if(arg1 == last){
            acc.recoveryPreviewLast();
        }
    }


    @Override
    public Integer getValue(LastValueAcc<Integer> accumulator) {
        Integer thisWindowLastValue = accumulator.getLastValue();

        return thisWindowLastValue;
    }


}
