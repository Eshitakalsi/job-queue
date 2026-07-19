package com.ekalsi.jobqueue;

import java.util.UUID;

/**
 * Provides a handler with everything it needs to process the current job.
 * Do not hold a reference to this object beyond the handle() call.
 */
public interface JobExecutionContext {

    UUID getJobId();
    String getJobType();
    /** 1-based: first attempt = 1, first retry = 2. */
    int getAttemptNumber();
    /** Raw JSON payload as supplied at submission time. */
    String getPayload();
    /**
     * Pushes the lease expiry forward. Call this inside long-running handlers
     * to prevent the stale-job reaper from reclaiming the job mid-execution.
     */
    void renewLease();
}
