package com.example.bulkinsert;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

@Component
public class BulkInserter {
    private static final Logger logger = LoggerFactory.getLogger(BulkInserter.class);

    private Connection connection;

    private void connect() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=TestDatabase", "sa", "SqlServer@2018");
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

    private void generateProductCsv(String fileName, int numProducts) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (int i = 0; i < numProducts; ++i) {
            String productName = RandomStringUtils.randomAlphabetic(10);
            writer.write(", " + productName);
            writer.newLine();
        }
        writer.flush();
    }

    // Sample code from https://stackoverflow.com/questions/40471004/can-i-get-bulk-insert-like-speeds-when-inserting-from-java-into-sql-server
    private void bulkInsertProductsCsv(Connection connection, String fileName, String tableName) throws Exception {
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

    private void generateInventoryCsv(String fileName, int firstProductId, int numProducts, int numInventoryPerProduct) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (int i = 0; i < numProducts; ++i) {
            for (int j = 0; j < numInventoryPerProduct; ++j) {
                int quantity = RandomUtils.nextInt(100);
                writer.write(", " + (firstProductId + i) + ", " + quantity);
                writer.newLine();
            }
        }
        writer.flush();
    }

    private void bulkInsertInventoryCsv(Connection connection, String fileName, String tableName) throws Exception {
        SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(fileName, false);
        fileRecord.addColumnMetadata(1, null, java.sql.Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(2, null, java.sql.Types.INTEGER, 0, 0);
        fileRecord.addColumnMetadata(3, null, java.sql.Types.INTEGER, 0, 0);
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
        int numInventoryPerProduct = 10;
        generateProductCsv("products.csv", numProducts);
        bulkInsertProductsCsv(connection,"products.csv", "products");
        int lastPk = getLastPK("products", "product_id");
        logger.info("Product PK: from " + (lastPk - numProducts + 1) + " to " + lastPk);
        generateInventoryCsv("inventory.csv", (lastPk - numProducts + 1), numProducts, numInventoryPerProduct);
        bulkInsertInventoryCsv(connection, "inventory.csv", "inventory");
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        int numRecords = numProducts * (1 + numInventoryPerProduct);
        logger.info("Time to generate and insert " + numRecords + " records: " + totalTime + "ms");
    }
}
