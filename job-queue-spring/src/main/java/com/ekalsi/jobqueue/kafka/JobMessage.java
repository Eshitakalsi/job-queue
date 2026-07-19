package com.ekalsi.jobqueue.kafka;

import java.util.UUID;

/** Kafka message payload. Key = jobId (for partition routing), value = JSON of this record. */
public record JobMessage(UUID jobId, String jobType) {}
