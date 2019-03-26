package com.example.bulkinsert.model;

import java.util.ArrayList;
import java.util.List;

public class DeviceDatalog {
    String[] fields;

    List<SensorDatalog> sensorDatalogs = new ArrayList<SensorDatalog>();

    /**
     * In production, DeviceDatalog is parsed from EventLog. For this POC, it is just parsed from the exported CSV since we will store data as CSV for bulk insert.
     */
    public static DeviceDatalog parseData(String row) {
        DeviceDatalog deviceDatalog = new DeviceDatalog();
        deviceDatalog.fields = row.split(",", 6);
        return deviceDatalog;
    }

    public void setId(String id) {
        fields[0] = id;
    }

    public String getId() {
        return fields[0];
    }

    public void addSensorDatalog(SensorDatalog sensorDatalog) {
        sensorDatalogs.add(sensorDatalog);
    }

    public String toCsvString() {
        return String.join(",", fields);
    }
}
