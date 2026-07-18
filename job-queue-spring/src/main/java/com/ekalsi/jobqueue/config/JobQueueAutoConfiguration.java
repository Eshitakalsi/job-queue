package com.ekalsi.jobqueue.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Central configuration class for the job queue engine.
 *
 * Any Spring Boot app that has job-queue-spring on its classpath will pick this
 * up automatically via component scanning (since @Configuration is a @Component).
 *
 * As we build the engine, all bean definitions — Kafka producer, consumer,
 * outbox poller, stale-job reaper — will be declared here.
 */
@Configuration
@EnableConfigurationProperties(JobQueueProperties.class)
public class JobQueueAutoConfiguration {

    /**
     * ObjectMapper used by the engine to serialize job payloads to JSON.
     * JavaTimeModule handles Java 8+ date/time types (Instant, LocalDate, etc.)
     * so they serialize as ISO strings rather than arrays of numbers.
     */
    @Bean
    public ObjectMapper jobQueueObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
