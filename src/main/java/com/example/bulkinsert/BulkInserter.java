package com.example.bulkinsert;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;

@Component
public class BulkInserter {
    private static final Logger logger = LoggerFactory.getLogger(BulkInserter.class);

    private Connection connection;

    private void connect() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=TestDatabase", "sa", "SqlServer@2018");
    }

    private void generateCsv(String fileName, int numProducts) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (int i = 0; i < numProducts; ++i) {
            String productName = RandomStringUtils.randomAlphabetic(10);
            writer.write(", " + productName);
            writer.newLine();
        }
        writer.flush();
    }

    // Sample code from https://stackoverflow.com/questions/40471004/can-i-get-bulk-insert-like-speeds-when-inserting-from-java-into-sql-server
    private void bulkInsertCsv(Connection connection, String fileName, String tableName) throws Exception {
        SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, false);
        fileRecord.addColumnMetadata(1, null, java.sql.Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(2, null, java.sql.Types.NVARCHAR, 50, 0);
        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();

        // Depending on the size of the data being uploaded, and the amount of RAM, an optimum can be found here. Play around with this to improve performance.
        copyOptions.setBatchSize(300000);

        // This is crucial to get good performance
        copyOptions.setTableLock(true);

        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableName);
        bulkCopy.writeToServer(fileRecord);
    }

    public void run() throws Exception {
        connect();
        long startTime = System.currentTimeMillis();
        int numProducts = 100000;
        generateCsv("products.csv", numProducts);
        bulkInsertCsv(connection,"products.csv", "products");
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        logger.info("Time to generate and insert " + numProducts + " records: " + totalTime + "ms");
    }
}
