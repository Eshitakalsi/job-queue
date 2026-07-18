package com.ekalsi.jobqueue;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable snapshot of a job's state at a point in time.
 *
 * This is the value object callers receive when they call getJob() or submit().
 * The mutable JPA entity that the engine writes to lives in job-queue-spring —
 * it is an internal implementation detail and never exposed outside the library.
 *
 * Java record: the compiler generates equals(), hashCode(), toString(), and
 * a canonical constructor automatically.
 */
public record Job(

    /** Stable identifier assigned at submission time. Use this to poll status. */
    UUID id,

    /** Matches the string key registered by a JobHandler — e.g. "SEND_EMAIL". */
    String type,

    /**
     * Arbitrary JSON string supplied by the caller at submission time.
     * The engine stores it as-is; the handler is responsible for deserializing it.
     */
    String payload,

    JobStatus status,

    /** How many times handle() has been called for this job so far (including the current attempt). */
    int attempts,

    /** Maximum number of handle() calls before the job moves to DEAD. */
    int maxAttempts,

    /** The exception message from the last failed attempt, or null if none yet. */
    String error,

    Instant createdAt,
    Instant updatedAt,

    /** Set when a worker first claims the job (status → IN_PROGRESS). */
    Instant startedAt,

    /** Set when the job reaches COMPLETED, DEAD, or CANCELLED. */
    Instant completedAt

) {}
