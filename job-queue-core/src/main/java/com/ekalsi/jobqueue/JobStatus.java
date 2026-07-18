package com.ekalsi.jobqueue;

/**
 * Every job moves forward through this state machine — never backwards.
 *
 *   PENDING → IN_PROGRESS → COMPLETED
 *                         ↘ FAILED → (retry) → IN_PROGRESS
 *                                  → (exhausted) → DEAD
 *   PENDING → CANCELLED
 */
public enum JobStatus {

    /** Submitted and waiting in Kafka. No worker has claimed it yet. */
    PENDING,

    /** A worker pod has claimed the job and is actively running the handler.
     *  A lease (expiry timestamp + pod identity) is held in the DB row. */
    IN_PROGRESS,

    /** The handler finished without throwing. Terminal — no further transitions. */
    COMPLETED,

    /** The handler threw an exception. The engine will retry if attempts < maxAttempts. */
    FAILED,

    /** Exhausted all retry attempts. Moved to the dead-letter Kafka topic for inspection.
     *  Terminal — the engine will not touch this job again. */
    DEAD,

    /** Caller cancelled the job while it was still PENDING.
     *  Terminal — a job that has already been claimed cannot be cancelled. */
    CANCELLED
}
