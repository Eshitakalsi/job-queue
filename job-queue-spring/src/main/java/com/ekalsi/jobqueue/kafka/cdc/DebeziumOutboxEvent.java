package com.ekalsi.jobqueue.kafka.cdc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DebeziumOutboxEvent(Payload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(String op, After after) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record After(
            UUID id,
            @JsonProperty("job_id")   UUID   jobId,
            @JsonProperty("job_type") String jobType
    ) {}
}
