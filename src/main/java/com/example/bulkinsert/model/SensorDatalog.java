package com.example.bulkinsert.model;

public class SensorDatalog {
    private String[] fields;

    /**
     * In production, SensorDatalog is parsed from EventLog. For this POC, it is parsed from the exported CSV.
     */
    public static SensorDatalog parseData(String row) {
        SensorDatalog sensorDatalog = new SensorDatalog();
        sensorDatalog.fields = row.split(",", 7);
        return sensorDatalog;
    }

    public void setDeviceDatalogId(String deviceDatalogId) {
        fields[3] = deviceDatalogId;
    }

    public String getDeviceDatalogId() {
        return fields[3];
    }

    public String toCsvString() {
        return String.join(",", fields);
    }
}
