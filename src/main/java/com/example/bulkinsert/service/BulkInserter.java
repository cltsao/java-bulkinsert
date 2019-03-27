package com.example.bulkinsert.service;

import com.example.bulkinsert.model.DeviceDatalog;
import com.example.bulkinsert.model.SensorDatalog;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.*;
import java.util.Collection;

@Component
public class BulkInserter {
    public enum Result {
        SUCCESS,
        DEVICE_DATALOG_FAILED,
        SENSOR_DATALOG_FAILED
    }

    private static final Logger logger = LoggerFactory.getLogger(BulkInserter.class);

    private Connection connection;

    @Value("${database.url}")
    private String databaseUrl;

    @Value("${database.username}")
    private String databaseUsername;

    @Value("${database.password}")
    private String databasePassword;

    public void init() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(databaseUrl, databaseUsername, databasePassword);
    }

    public int getLastPK(String tableName, String pkName) throws Exception {
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT max(" + pkName + ") as lastPK FROM " + tableName);
        if (rs.next()) {
            String lastPK = rs.getString("lastPK");
            if (!StringUtils.isEmpty(lastPK)) {
                return Integer.parseInt(lastPK);
            }
        }

        logger.info("No record found. Assume last PK to be 0");
        return 0;
    }

    public void generateDeviceDatalogCsv(String fileName, Collection<DeviceDatalog> deviceDatalogs) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for(DeviceDatalog deviceDatalog : deviceDatalogs) {
            writer.write(deviceDatalog.toCsvString());
            writer.newLine();
        }
        writer.flush();
    }

    // Sample code from https://stackoverflow.com/questions/40471004/can-i-get-bulk-insert-like-speeds-when-inserting-from-java-into-sql-server
    public void bulkInsertDeviceDatalogs(String fileName, String tableName) throws Exception {
        SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, false);
        fileRecord.addColumnMetadata(1, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(2, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(3, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(4, null, Types.DATE, 0, 0);
        fileRecord.addColumnMetadata(5, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(6, null, Types.VARBINARY, 0, 0);
        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();

        // This is crucial to get good performance
        copyOptions.setTableLock(true);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableName);
        bulkCopy.writeToServer(fileRecord);
    }

    public void generateSensorDatalog(String fileName, int firstDeviceDatalogId, Collection<DeviceDatalog> deviceDatalogs) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        int deviceDatalogId = firstDeviceDatalogId;
        for(DeviceDatalog deviceDatalog : deviceDatalogs) {
            deviceDatalog.setId(String.valueOf(deviceDatalogId));
            for(SensorDatalog sensorDatalog : deviceDatalog.sensorDatalogs) {
                writer.write(sensorDatalog.toCsvString());
                writer.newLine();
            }
            ++deviceDatalogId;
        }
        writer.flush();
    }

    public void bulkInsertSensorDatalogs(String fileName, String tableName) throws Exception {
        SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, false);
        fileRecord.addColumnMetadata(1, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(2, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(3, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(4, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(5, null, Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(6, null, Types.FLOAT, 0, 0);
        fileRecord.addColumnMetadata(7, null, Types.VARBINARY, 0, 0);
        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();

        // This is crucial to get good performance
        copyOptions.setTableLock(true);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableName);
        bulkCopy.writeToServer(fileRecord);
    }


}
