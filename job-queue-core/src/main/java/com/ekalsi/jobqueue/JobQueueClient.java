package com.ekalsi.jobqueue;

import java.util.UUID;

/** Primary API for submitting and querying jobs. */
public interface JobQueueClient {

    /** Submit using the default max-attempts from configuration. */
    Job submit(String type, Object payload);

    /** Submit with an explicit retry cap (total attempts including the first). */
    Job submit(String type, Object payload, int maxAttempts);

    /** @throws JobNotFoundException if no job exists with this ID */
    Job getJob(UUID jobId);

    /**
     * Cancel a PENDING job.
     * @return true if canceled; false if already IN_PROGRESS, COMPLETED, DEAD, or CANCELLED.
     */
    boolean cancel(UUID jobId);
}
