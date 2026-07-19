package com.ekalsi.jobqueue;

/**
 * Contract for processing a specific job type.
 *
 * Register a handler by creating a @Component that implements this interface.
 * The engine discovers all handlers on startup and routes by job type.
 *
 * Handlers MUST be idempotent — the engine guarantees at-least-once delivery,
 * so handle() may be called more than once for the same jobId.
 * Throw any exception to signal failure; the engine handles retry and dead-lettering.
 */
public interface JobHandler {

    /** Must exactly match the "type" field used at submission time. */
    String getJobType();

    void handle(JobExecutionContext context) throws Exception;
}
