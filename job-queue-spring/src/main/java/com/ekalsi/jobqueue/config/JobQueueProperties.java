package com.ekalsi.jobqueue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "job-queue")
public record JobQueueProperties(

        @DefaultValue("job-queue-workers")
        String consumerGroup,

        @DefaultValue
        KafkaProperties kafka,

        @DefaultValue
        CdcProperties cdc,

        @DefaultValue
        LeaseProperties lease,

        @DefaultValue
        ReaperProperties reaper,

        @DefaultValue
        DefaultJobProperties defaults

) {

    public record KafkaProperties(

            /** Optional prefix for all topic names, e.g. "payments" → "payments.jobs.created". */
            @DefaultValue("")
            String topicPrefix,

            @DefaultValue("jobs.created")
            String jobsTopic,

            @DefaultValue("jobs.dead")
            String deadLetterTopic,

            /** Number of concurrent listener threads per pod. Should not exceed partition count. */
            @DefaultValue("3")
            int concurrency

    ) {
        public String resolvedJobsTopic() {
            return topicPrefix.isBlank() ? jobsTopic : topicPrefix + "." + jobsTopic;
        }

        public String resolvedDeadLetterTopic() {
            return topicPrefix.isBlank() ? deadLetterTopic : topicPrefix + "." + deadLetterTopic;
        }
    }

    public record CdcProperties(

            @DefaultValue("job-queue-db.public.job_outbox")
            String outboxTopic

    ) {}

    public record LeaseProperties(

            /**
             * Seconds a worker holds a job before the reaper can reclaim it.
             * Set above your longest expected handler execution time.
             */
            @DefaultValue("60")
            int durationSeconds

    ) {}

    public record ReaperProperties(

            @DefaultValue("30")
            int intervalSeconds

    ) {}

    public record DefaultJobProperties(

            /** Total handle() calls allowed, including the first attempt. */
            @DefaultValue("3")
            int maxAttempts

    ) {}
}
