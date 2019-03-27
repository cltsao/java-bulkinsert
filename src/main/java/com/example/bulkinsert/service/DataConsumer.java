package com.example.bulkinsert.service;

import com.example.bulkinsert.model.DeviceDatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * This builds batches of datalogs to pass to bulk insert. This is implemented as a thread so that bulk insert can be done without loading full data into memory. In production, this can be done from the main thread.
 */
@Component
public class DataConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DataConsumer.class);

    @Autowired
    DataInserter dataInserter;

    @Value("${batch.size}")
    private int batchSize;

    Queue<DeviceDatalog> queue = new ArrayDeque<>();

    /**
     * Send datalogs using bulk insert.
     */
    private void sendDatalogs() {
        BulkInserter.Result result = dataInserter.send(queue);
        if (result != BulkInserter.Result.SUCCESS)
            onError(result);
        queue.clear();
    }

    /**
     * Placeholder when error occurs in bulk insert. Fall back to the existing data pipeline.
     */
    private void onError(BulkInserter.Result result) {
        logger.warn("Error in sending " + queue.size() + " device datalogs starting from " + queue.peek().getId() + ": " + result);
        switch(result) {
            case DEVICE_DATALOG_FAILED:
                logger.warn("Send device datalogs and sensor datalogs using existing data pipeline");
                break;
            case SENSOR_DATALOG_FAILED:
                logger.warn("Send sensor datalogs using existing data pipeline");
                break;
        }
    }

    private long startTime;

    public void init() throws Exception {
        dataInserter.init();
        startTime = System.currentTimeMillis();
    }

    public void consume(DeviceDatalog deviceDatalog) {
        queue.add(deviceDatalog);
        if (batchSize > 0 && queue.size() >= batchSize) {
            sendDatalogs();
        }
    }

    /**
     * Send the remaining datalogs.
     */
    public void flush() {
        sendDatalogs();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        logger.info("Total time of bulk insert with batch size " + batchSize + ": " + totalTime + "ms");
    }
}
