package com.example.bulkinsert.service;

import com.example.bulkinsert.model.DeviceDatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * The logic of data insertion that depends on the data model. This only captures the relationship between DeviceDatalog and SensorDatalog.
 */
@Component
public class DataInserter {
    private static final Logger logger = LoggerFactory.getLogger(DataInserter.class);

    @Autowired BulkInserter bulkInserter;

    public void init() throws Exception {
        bulkInserter.init();
    }

    /**
     * Need to distinguish exception when inserting device datalogs or sensor datalogs, then notify the data consumer to fall back to original data pineline only for the failed dataset to avoid duplication.
     */
    public BulkInserter.Result send(Collection<DeviceDatalog> deviceDatalogs) {
        try {
            bulkInserter.generateDeviceDatalogCsv("deviceDatalog.csv", deviceDatalogs);
            bulkInserter.bulkInsertDeviceDatalogs("deviceDatalog.csv", "FOG_DeviceDatalogRecord");
        } catch (Exception ex) {
            logger.warn("Bulk insert of device datalog failed", ex);
            return BulkInserter.Result.DEVICE_DATALOG_FAILED;
        }
        try {
            int lastPk = bulkInserter.getLastPK("FOG_DeviceDatalogRecord", "ID");
            bulkInserter.generateSensorDatalog("sensorDatalog.csv", (lastPk - deviceDatalogs.size() + 1), deviceDatalogs);
            bulkInserter.bulkInsertSensorDatalogs("sensorDatalog.csv", "FOG_SensorDatalogRecord");
        } catch (Exception ex) {
            logger.warn("Bulk insert of sensor datalog failed", ex);
            return BulkInserter.Result.SENSOR_DATALOG_FAILED;
        }
        return BulkInserter.Result.SUCCESS;
    }
}
