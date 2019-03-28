package com.example.bulkinsert.service;

import com.example.bulkinsert.model.DataRow;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Component
public class BulkInserter {

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

    private int getLastPK(String tableName, String pkName) throws Exception {
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

    /**
     * Pass on Exceptions in Stream processing for now.
     *
     * @see https://www.baeldung.com/java-lambda-exceptions
     */
    @FunctionalInterface
    public interface ThrowingConsumer<T, E extends Exception> {
        void accept(T t) throws E;
    }

    static <T> Consumer<T> throwingConsumerWrapper(
            ThrowingConsumer<T, Exception> throwingConsumer) {

        return i -> {
            try {
                throwingConsumer.accept(i);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

    private <T extends DataRow> void generateCsvFile(String fileName, Stream<T> rows) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        rows.forEachOrdered(throwingConsumerWrapper(row -> {
            writer.write(row.toCsvString());
            writer.newLine();
        }));
        writer.flush();
    }

    private void setColumnMetadata(SQLServerBulkCSVFileRecord fileRecord, int[] dataTypes) throws SQLServerException {
        for (int i = 0; i < dataTypes.length; ++i) {
            fileRecord.addColumnMetadata(i + 1, null, dataTypes[i], 0, 0);  // Column index starts with 1
        }
    }

    // Sample code from https://stackoverflow.com/questions/40471004/can-i-get-bulk-insert-like-speeds-when-inserting-from-java-into-sql-server
    private void bulkInsertFromFile(String fileName, int[] dataTypes, String tableName) throws Exception {
        SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, false);
        setColumnMetadata(fileRecord, dataTypes);

        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();

        // This is crucial to get good performance
        copyOptions.setTableLock(true);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableName);
        bulkCopy.writeToServer(fileRecord);
    }

    private <T extends DataRow> void doBulkInsertWithCsvFile(Stream<T> rows, int[] dataTypes, String tableName) throws Exception {
        String fileName = tableName + ".csv";
        generateCsvFile(fileName, rows);
        bulkInsertFromFile(fileName, dataTypes, tableName);
    }

    private <T extends DataRow> InputStream generateCsvInputStream(Stream<T> rows) throws IOException {
        PipedInputStream pipedInputStream = new PipedInputStream();
        PipedOutputStream pipedOutputStream = new PipedOutputStream();
        pipedInputStream.connect(pipedOutputStream);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(pipedOutputStream));
        Thread dataWriter = new Thread(() -> {
            try {
                rows.forEachOrdered(throwingConsumerWrapper(row -> {
                    writer.write(row.toCsvString());
                    writer.newLine();
                }));

                writer.flush();
                pipedOutputStream.close();
                // logger.info("Finished writing CSV content to piped stream");
            } catch (Exception ex) {
                logger.warn("Exception in writing to piped stream", ex);
            }
        });
        dataWriter.start();
        return pipedInputStream;
    }

    // Sample code from https://myadventuresincoding.wordpress.com/2018/04/01/java-using-sqlserverbulkcopy-in-java-with-an-inputstream/
    private void bulkInsertFromInputStream(InputStream inputStream, int[] dataTypes, String tableName) throws Exception {
        SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(inputStream, StandardCharsets.UTF_8.name(), ",", false);
        setColumnMetadata(fileRecord, dataTypes);

        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();

        // This is crucial to get good performance
        copyOptions.setTableLock(true);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableName);
        bulkCopy.writeToServer(fileRecord);
        // logger.info("Finished bulk insert");
    }

    private <T extends DataRow> void doBulkInsertWithPipedStream(Stream<T> rows, int[] dataTypes, String tableName) throws Exception {
        InputStream inputStream = generateCsvInputStream(rows);
        bulkInsertFromInputStream(inputStream, dataTypes, tableName);
        inputStream.close();
    }

    /**
     * Performs bulk insert.
     *
     * @param rows      Data input for bulk insert.
     * @param dataTypes Column data type in java.sql.Types.
     * @param tableName Name of the table to do bulk insert.
     * @return Last PK of the record after successful bulk insert.
     * @throws Exception
     */
    public <T extends DataRow> int bulkInsert(Stream<T> rows, int[] dataTypes, String tableName, String pkName) throws Exception {
        doBulkInsertWithCsvFile(rows, dataTypes, tableName);
        // doBulkInsertWithPipedStream(rows, dataTypes, tableName);
        return getLastPK(tableName, pkName);
    }
}
