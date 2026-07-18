package com.ekalsi.jobqueue;

import java.util.UUID;

/**
 * The primary API for submitting and querying jobs.
 *
 * Services that add job-queue-spring as a dependency get an instance of this
 * auto-wired into their beans by Spring. There is no HTTP call involved —
 * submit() writes directly to the local DB in the same JVM.
 *
 * The implementation in job-queue-spring inserts the job row and an outbox row
 * in a single DB transaction, guaranteeing the job is never silently lost even
 * if the Kafka publish step fails.
 */
public interface JobQueueClient {

    /**
     * Submit a job using the default max-attempts from configuration
     * (job-queue.defaults.max-attempts, defaults to 3).
     *
     * @param type     job type key — must match a registered JobHandler.getJobType()
     * @param payload  any object; the engine serializes it to JSON
     * @return         the created job with status = PENDING
     */
    Job submit(String type, Object payload);

    /**
     * Submit a job with an explicit retry cap.
     *
     * @param maxAttempts  total number of times handle() may be called, including the first attempt
     */
    Job submit(String type, Object payload, int maxAttempts);

    /**
     * Fetch the current state of a job by its ID.
     *
     * @throws JobNotFoundException  if no job exists with this ID
     */
    Job getJob(UUID jobId);

    /**
     * Cancel a PENDING job.
     *
     * @return true if the job was successfully cancelled;
     *         false if it was already IN_PROGRESS, COMPLETED, FAILED, DEAD, or CANCELLED.
     */
    boolean cancel(UUID jobId);
}
