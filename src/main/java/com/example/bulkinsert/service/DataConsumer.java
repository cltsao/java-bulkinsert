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
    BulkInserter bulkInserter;

    @Value("${batch.size}")
    private int batchSize;

    Queue<DeviceDatalog> queue = new ArrayDeque<>();

    /**
     * Send datalogs using bulk insert.
     */
    private void sendDatalogs() {
        try {
            bulkInserter.send(queue);
        } catch (Exception ex) {
            logger.warn("Error in sending datalogs: ", ex);
            onError();
        }
        queue.clear();
    }

    /**
     * Placeholder when error occurs in bulk insert. Fall back to the existing data pipeline.
     */
    private void onError() {
        logger.warn("Error in sending " + queue.peek() + " device datalogs starting from " + queue.peek().getId());
        queue.clear();
    }

    private long startTime;

    public void init() throws Exception {
        bulkInserter.init();
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
