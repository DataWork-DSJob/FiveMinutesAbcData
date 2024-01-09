package com.bigdata.streaming.flink.mystudy.streamsql.typeschema;

public class Device{
     String orgId;
     String deviceId;


    @Override
    public String toString() {
        String str = "{" + orgId + ',' + deviceId + '}';
        return str;
    }

    @Override
    public int hashCode() {
        String str = String.join("_", orgId, deviceId);
        return str.hashCode();
    }

}

