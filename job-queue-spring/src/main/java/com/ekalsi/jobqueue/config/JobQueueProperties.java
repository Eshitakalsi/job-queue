package com.ekalsi.jobqueue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Typed binding for all job-queue.* properties.
 *
 * Every nested group is a Java record — Spring binds the YAML/properties
 * fields to the record components automatically.
 *
 * Usage in application.yml:
 *
 *   job-queue:
 *     consumer-group: my-app-job-workers
 *     kafka:
 *       jobs-topic: jobs.created
 *       concurrency: 3
 *     lease:
 *       duration-seconds: 60
 *     defaults:
 *       max-attempts: 3
 *
 * Any property not set falls back to the @DefaultValue declared here.
 */
@ConfigurationProperties(prefix = "job-queue")
public record JobQueueProperties(

        /**
         * Kafka consumer group name. All worker pods of the same application
         * should share one group so Kafka distributes partitions among them.
         * Default: "job-queue-workers"
         */
        @DefaultValue("job-queue-workers")
        String consumerGroup,

        @DefaultValue
        KafkaProperties kafka,

        @DefaultValue
        OutboxProperties outbox,

        @DefaultValue
        LeaseProperties lease,

        @DefaultValue
        ReaperProperties reaper,

        @DefaultValue
        DefaultJobProperties defaults

) {

    // ── Kafka ─────────────────────────────────────────────────────────────────
    public record KafkaProperties(

            /**
             * Optional prefix prepended to every topic name.
             * Useful when multiple applications share the same Kafka cluster:
             *   topic-prefix: "payments"  →  topic = "payments.jobs.created"
             */
            @DefaultValue("")
            String topicPrefix,

            /** Topic workers consume from. Full name = topicPrefix + jobsTopic. */
            @DefaultValue("jobs.created")
            String jobsTopic,

            /** Jobs that exhaust all retries are published here for manual inspection. */
            @DefaultValue("jobs.dead")
            String deadLetterTopic,

            /**
             * Number of concurrent Kafka listener threads per pod.
             * Each thread processes one partition at a time.
             * Set this <= number of partitions on the topic, otherwise extra threads idle.
             */
            @DefaultValue("3")
            int concurrency

    ) {
        /** Resolves the full topic name including any prefix. */
        public String resolvedJobsTopic() {
            return topicPrefix.isBlank() ? jobsTopic : topicPrefix + "." + jobsTopic;
        }

        public String resolvedDeadLetterTopic() {
            return topicPrefix.isBlank() ? deadLetterTopic : topicPrefix + "." + deadLetterTopic;
        }
    }

    // ── Outbox poller ─────────────────────────────────────────────────────────
    public record OutboxProperties(

            /** How often the outbox poller wakes up to flush unpublished rows to Kafka. */
            @DefaultValue("1000")
            long pollIntervalMs,

            /** Max rows the poller reads and publishes per tick. */
            @DefaultValue("100")
            int batchSize

    ) {}

    // ── Lease ─────────────────────────────────────────────────────────────────
    public record LeaseProperties(

            /**
             * How long (in seconds) a worker pod holds a job before the reaper
             * can reclaim it. Set this comfortably above your longest expected
             * handler execution time, or call context.renewLease() inside the handler.
             */
            @DefaultValue("60")
            int durationSeconds

    ) {}

    // ── Stale-job reaper ──────────────────────────────────────────────────────
    public record ReaperProperties(

            /** How often the reaper scans for expired leases. */
            @DefaultValue("30")
            int intervalSeconds

    ) {}

    // ── Job defaults ──────────────────────────────────────────────────────────
    public record DefaultJobProperties(

            /**
             * Default retry limit applied when the caller does not specify maxAttempts.
             * Total handle() calls including the first attempt:
             *   maxAttempts=3 → 1 original + 2 retries, then DEAD.
             */
            @DefaultValue("3")
            int maxAttempts

    ) {}
}
