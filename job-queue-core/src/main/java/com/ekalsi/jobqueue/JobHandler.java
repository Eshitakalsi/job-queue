package com.ekalsi.jobqueue;

/**
 * Contract that every job handler must implement.
 *
 * Adding support for a new job type = creating one @Component that implements this interface.
 * The engine auto-discovers all JobHandler beans on startup and routes jobs to the right one
 * based on job.type == handler.getJobType().
 *
 * --- Idempotency requirement ---
 * The engine guarantees at-least-once delivery. handle() may be called more than once
 * for the same job (e.g. if a worker pod crashes after starting but before committing).
 * Handlers MUST be safe to call multiple times with the same jobId without causing
 * duplicate side-effects — e.g. check if the email was already sent before sending it.
 *
 * --- Error handling ---
 * Throw any exception to signal failure. Do NOT catch and swallow exceptions —
 * the engine needs to see the failure to trigger retry / dead-letter logic.
 */
public interface JobHandler {

    /**
     * The job type string this handler processes.
     * Must exactly match the "type" field supplied at job submission time.
     * Convention: SCREAMING_SNAKE_CASE, e.g. "SEND_EMAIL", "GENERATE_REPORT".
     */
    String getJobType();

    /**
     * Execute the job.
     *
     * @param context  carries jobId, payload, attempt number, and a lease-renewal callback.
     * @throws Exception  any exception signals failure; the engine handles the rest.
     */
    void handle(JobExecutionContext context) throws Exception;
}
