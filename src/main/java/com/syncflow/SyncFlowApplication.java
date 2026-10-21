package com.syncflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication
public class SyncFlowApplication {

    public static void main(String[] args) {
        // Fix Windows default "Asia/Calcutta" issue for PostgreSQL
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        SpringApplication.run(SyncFlowApplication.class, args);
    }
}