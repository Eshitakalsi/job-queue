package com.ekalsi.jobqueue;

import java.util.UUID;

/**
 * Everything a handler needs to know about the job it is processing.
 *
 * The engine creates an instance of this and passes it to JobHandler.handle().
 * Handlers should NOT hold a reference to this object beyond the handle() call —
 * the lease renewal callback is only valid while the job is IN_PROGRESS.
 */
public interface JobExecutionContext {

    UUID getJobId();

    String getJobType();

    /**
     * 1-based attempt counter.
     * First attempt = 1, first retry = 2, and so on.
     * Handlers can use this to implement exponential back-off or
     * to skip expensive side-effects on later retries.
     */
    int getAttemptNumber();

    /**
     * The raw JSON payload exactly as supplied at submission time.
     * Deserialize using Jackson, Gson, or any library you prefer.
     */
    String getPayload();

    /**
     * Pushes the lease expiry forward by the configured lease duration.
     * Call this periodically inside long-running handlers to prevent the
     * stale-job reaper from reclaiming the job and re-queueing it.
     *
     * Rule of thumb: call renewLease() every (leaseDuration / 4) seconds.
     */
    void renewLease();
}
