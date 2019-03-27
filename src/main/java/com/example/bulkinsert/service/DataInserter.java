package com.example.bulkinsert.service;

import com.example.bulkinsert.model.DeviceDatalog;
import com.example.bulkinsert.model.SensorDatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.Types;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * The logic of data insertion that depends on the data model. This only captures the relationship between DeviceDatalog and SensorDatalog.
 */
@Component
public class DataInserter {
    private static final Logger logger = LoggerFactory.getLogger(DataInserter.class);

    @Autowired
    BulkInserter bulkInserter;

    public enum Result {
        SUCCESS,
        DEVICE_DATALOG_FAILED,
        SENSOR_DATALOG_FAILED
    }

    public void init() throws Exception {
        bulkInserter.init();
    }

    /**
     * Need to distinguish exception when inserting device datalogs or sensor datalogs, then notify the data consumer to fall back to original data pineline only for the failed dataset to avoid duplication.
     */
    public Result send(Collection<DeviceDatalog> deviceDatalogs) {
        int lastPk = -1;
        try {
            int[] dataTypes = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.DATE, Types.INTEGER, Types.VARBINARY};
            lastPk = bulkInserter.bulkInsert(deviceDatalogs.stream(), dataTypes, "FOG_DeviceDatalogRecord", "ID");
        } catch (Exception ex) {
            logger.warn("Bulk insert of device datalog failed", ex);
            return Result.DEVICE_DATALOG_FAILED;
        }
        try {
            int[] dataTypes = {Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.FLOAT, Types.VARBINARY};
            Stream<SensorDatalog> sensorDatalogs = deviceDatalogs.stream().flatMap(deviceDatalog -> deviceDatalog.sensorDatalogs.stream());
            bulkInserter.bulkInsert(sensorDatalogs, dataTypes, "FOG_SensorDatalogRecord", "ID");
        } catch (Exception ex) {
            logger.warn("Bulk insert of sensor datalog failed", ex);
            return Result.SENSOR_DATALOG_FAILED;
        }
        return Result.SUCCESS;
    }
}
