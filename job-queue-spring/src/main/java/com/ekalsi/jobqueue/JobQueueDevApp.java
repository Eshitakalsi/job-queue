package com.ekalsi.jobqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Development-only entry point for running the engine as a standalone app.
 * In production, consuming services provide their own @SpringBootApplication
 * and get the engine wired in through Spring Boot auto-configuration.
 */
@SpringBootApplication
public class JobQueueDevApp {

    public static void main(String[] args) {
        SpringApplication.run(JobQueueDevApp.class, args);
    }
}
