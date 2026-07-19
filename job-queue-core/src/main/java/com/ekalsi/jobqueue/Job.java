package com.ekalsi.jobqueue;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable snapshot of a job returned to callers by JobQueueClient.
 */
public record Job(
        UUID id,
        String type,
        // Raw JSON string as supplied at submission time.
        String payload,
        JobStatus status,
        int attempts,
        int maxAttempts,
        //Exception message from the last failed attempt, or null.
        String error,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt
) {
}
