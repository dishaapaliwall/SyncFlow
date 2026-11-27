package com.syncflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.TimeZone;

@SpringBootApplication
public class SyncFlowApplication {

    public static void main(String[] args) {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        // Automatically load .env file into Java System Properties
        loadEnv();

        SpringApplication.run(SyncFlowApplication.class, args);
    }

    private static void loadEnv() {
        File envFile = new File(".env");
        if (!envFile.exists()) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(envFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                int eqIndex = line.indexOf('=');
                if (eqIndex > 0) {
                    String key = line.substring(0, eqIndex).trim();
                    String value = line.substring(eqIndex + 1).trim();

                    // Strip optional quotes ("value" or 'value')
                    if ((value.startsWith("\"") && value.endsWith("\"")) ||
                            (value.startsWith("'") && value.endsWith("'"))) {
                        value = value.substring(1, value.length() - 1);
                    }

                    System.setProperty(key, value);
                }
            }
        } catch (IOException e) {
            System.err.println("Warning: Could not read .env: " + e.getMessage());
        }
    }
}