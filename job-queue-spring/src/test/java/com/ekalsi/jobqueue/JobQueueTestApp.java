package com.ekalsi.jobqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Run: mvn spring-boot:test-run -pl job-queue-spring
 */
@SpringBootApplication
public class JobQueueTestApp {
    public static void main(String[] args) {
        SpringApplication.run(JobQueueTestApp.class, args);
    }
}
