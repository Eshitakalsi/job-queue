package com.ekalsi.jobqueue;

import java.util.UUID;

/**
 * Thrown when a job ID is looked up but does not exist in the store.
 * Unchecked so callers are not forced to handle it if they are confident the ID is valid.
 */
public class JobNotFoundException extends RuntimeException {

    public JobNotFoundException(UUID jobId) {
        super("No job found with id: " + jobId);
    }
}
