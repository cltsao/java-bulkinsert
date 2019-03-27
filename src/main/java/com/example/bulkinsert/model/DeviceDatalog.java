package com.example.bulkinsert.model;

import java.util.ArrayList;
import java.util.List;

public class DeviceDatalog extends DataRow {
    public List<SensorDatalog> sensorDatalogs = new ArrayList<SensorDatalog>();

    /**
     * In production, DeviceDatalog is parsed from EventLog. For this POC, it is just parsed from the exported CSV since we will store data as CSV for bulk insert.
     */
    public static DeviceDatalog parseData(String row) {
        DeviceDatalog deviceDatalog = new DeviceDatalog();
        deviceDatalog.fields = row.split(",", 6);
        deviceDatalog.replaceNulls();
        return deviceDatalog;
    }

    public String getId() {
        return fields[0];
    }

    public void setId(String id) {
        fields[0] = id;
        for(SensorDatalog sensorDatalog : sensorDatalogs) {
            sensorDatalog.setDeviceDatalogId(id);
        }
    }

    public void addSensorDatalog(SensorDatalog sensorDatalog) {
        sensorDatalogs.add(sensorDatalog);
    }
}
