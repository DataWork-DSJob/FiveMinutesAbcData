package com.bigdata.streaming.flink.mystudy.streamsql.sqlfunction.windowed.simpletest;

public class LastValueAcc<T> {

    private T lastValue;
    private T previewLastValue;

    public LastValueAcc(T lastValue) {
        this.lastValue = lastValue;
    }

    public void setLastValue(T newValue) {
        this.previewLastValue = this.lastValue;
        this.lastValue = newValue;
    }

    public void recoveryPreviewLast() {
        this.lastValue = this.previewLastValue;
        this.previewLastValue = null;
    }

    public T getLastValue() {
        return lastValue;
    }

    public T getPreviewLastValue() {
        return previewLastValue;
    }
}
