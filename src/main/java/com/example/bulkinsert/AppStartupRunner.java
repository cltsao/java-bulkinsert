package com.example.bulkinsert;

import com.example.bulkinsert.service.BulkInserter;
import com.example.bulkinsert.service.DataConsumer;
import com.example.bulkinsert.service.DataProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class AppStartupRunner implements ApplicationRunner {
    @Autowired
    DataProducer datalogProducer;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        datalogProducer.start();
    }
}