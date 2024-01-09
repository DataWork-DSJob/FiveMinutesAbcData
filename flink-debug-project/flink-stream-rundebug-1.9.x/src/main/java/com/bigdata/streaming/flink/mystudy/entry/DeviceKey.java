package com.github.bigdata.flink.stream.entry;

public class DeviceKey {
    private String orgId;
    private String deviceId;
    private int keyHashKey;

    public DeviceKey(String orgId, String deviceId) {
        this.orgId = orgId;
        this.deviceId = deviceId;
        String str = String.join("_", orgId, deviceId);
        this.keyHashKey = str.hashCode();
    }

    public DeviceKey(String assetId) {
        this("o15578033973151",assetId);
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    @Override
    public String toString() {
        String str = "{" + orgId + ',' + deviceId + '}';
        return str;
    }

    @Override
    public int hashCode() {
        return keyHashKey;
    }

    @Override
    public boolean equals(Object obj) {
        return this.keyHashKey== obj.hashCode();
    }
}
