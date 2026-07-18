package com.ekalsi.jobqueue.controller;

/**
 * Request body for POST /jobs.
 *
 * payload is Object so Jackson accepts any valid JSON structure —
 * objects, arrays, strings, numbers. Jackson deserializes it into
 * Map/List/primitives, and JobQueueClientImpl re-serializes it to a
 * JSON string for storage in the JSONB column.
 *
 * maxAttempts is Integer (nullable) — null means "use the configured default".
 */
public record JobRequest(
        String type,
        Object payload,
        Integer maxAttempts
) {}
