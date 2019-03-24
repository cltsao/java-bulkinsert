package com.example.bulkinsert;

import com.microsoft.sqlserver.jdbc.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class BulkInserter {
    private static final Logger logger = LoggerFactory.getLogger(BulkInserter.class);

    public void run() {
        try {
            long startTime = System.currentTimeMillis();
            SQLServerBulkCSVFileRecord fileRecord = null;

            fileRecord = new SQLServerBulkCSVFileRecord("test.csv", true);
            fileRecord.addColumnMetadata(1, null, java.sql.Types.NVARCHAR, 50, 0);
            fileRecord.addColumnMetadata(2, null, java.sql.Types.INTEGER, 0, 0);
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection destinationConnection = DriverManager.getConnection("jdbc:sqlserver://localhost:1433;databaseName=TestDatabase", "sa", "SqlServer@2018");
            SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();

            // Depending on the size of the data being uploaded, and the amount of RAM, an optimum can be found here. Play around with this to improve performance.
            copyOptions.setBatchSize(300000);

            // This is crucial to get good performance
            copyOptions.setTableLock(true);

            SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(destinationConnection);
            bulkCopy.setBulkCopyOptions(copyOptions);
            bulkCopy.setDestinationTableName("TestTable");
            bulkCopy.writeToServer(fileRecord);

            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println(totalTime + "ms");
        } catch (SQLException ex) {
            logger.warn("SQLException", ex);
        } catch (ClassNotFoundException ex) {
            logger.warn("Class not found", ex);
        }
    }
}
