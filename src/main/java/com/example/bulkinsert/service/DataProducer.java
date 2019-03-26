package com.example.bulkinsert.service;

import com.example.bulkinsert.model.DeviceDatalog;
import com.example.bulkinsert.model.SensorDatalog;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * This represents the producer of data. It can be BWClipLogParser that parses events into sensor data log.
 */
@Component
public class DataProducer {
    private static final Logger logger = LoggerFactory.getLogger(DataProducer.class);

    @Autowired
    DataConsumer datalogConsumer;

    @Value("${device.data.input}")
    private String deviceDataInput;

    @Value("${sensor.data.input}")
    private String sensorDataInput;

    private SortedMap<String, DeviceDatalog> deviceDatalogs = new TreeMap<>();

    private void onProducing(DeviceDatalog deviceDatalog) {
        datalogConsumer.consume(deviceDatalog);
    }

    private void loadData() {
        int countSensorDatalogs = 0;
        try (BufferedReader deviceReader = new BufferedReader(new FileReader(deviceDataInput));
             BufferedReader sensorReader = new BufferedReader(new FileReader(sensorDataInput))) {
            String deviceData = deviceReader.readLine();  // Ignore header row
            while ((deviceData = deviceReader.readLine()) != null && !StringUtils.isBlank(deviceData)) {
                DeviceDatalog deviceDatalog = DeviceDatalog.parseData(deviceData);
                deviceDatalogs.put(deviceDatalog.getId(), deviceDatalog);
            }

            String sensorData = sensorReader.readLine();  // Ignore header row
            while ((sensorData = sensorReader.readLine()) != null && !StringUtils.isBlank(sensorData)) {
                SensorDatalog sensorDatalog = SensorDatalog.parseData(sensorData);
                if (!deviceDatalogs.containsKey(sensorDatalog.getDeviceDatalogId()))
                    logger.warn("No device datalog with ID " + sensorDatalog.getDeviceDatalogId());

                deviceDatalogs.get(sensorDatalog.getDeviceDatalogId()).addSensorDatalog(sensorDatalog);
                ++countSensorDatalogs;
            }
        } catch (Exception ex) {
            logger.warn("Error in reading input files", ex);
        }
        logger.info("Loaded " + deviceDatalogs.size() + " device datalogs");
        logger.info("Loaded " + countSensorDatalogs + " sensor datalogs");
    }

    public void start() throws Exception {
        loadData();
        datalogConsumer.init();
        for(String deviceDatalogId : deviceDatalogs.keySet()) {
            datalogConsumer.consume(deviceDatalogs.get(deviceDatalogId));
        }
        datalogConsumer.flush();
    }
}
