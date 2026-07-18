package com.ekalsi.jobqueue.service;

import com.ekalsi.jobqueue.Job;
import com.ekalsi.jobqueue.JobNotFoundException;
import com.ekalsi.jobqueue.JobQueueClient;
import com.ekalsi.jobqueue.config.JobQueueProperties;
import com.ekalsi.jobqueue.entity.JobEntity;
import com.ekalsi.jobqueue.entity.JobOutboxEntity;
import com.ekalsi.jobqueue.repository.JobOutboxRepository;
import com.ekalsi.jobqueue.repository.JobRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Implements the JobQueueClient contract using JPA + the transactional outbox pattern.
 *
 * The critical invariant: a job row and its outbox row are always created together
 * in one DB transaction. Either both exist or neither does — there is no window
 * where a job is in the DB but silently missing from Kafka.
 */
@Service
public class JobQueueClientImpl implements JobQueueClient {

    private final JobRepository jobRepository;
    private final JobOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final JobQueueProperties properties;

    public JobQueueClientImpl(JobRepository jobRepository,
                              JobOutboxRepository outboxRepository,
                              ObjectMapper objectMapper,
                              JobQueueProperties properties) {
        this.jobRepository   = jobRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper    = objectMapper;
        this.properties      = properties;
    }

    @Override
    @Transactional
    public Job submit(String type, Object payload) {
        return submit(type, payload, properties.defaults().maxAttempts());
    }

    /**
     * The core submit flow:
     *   1. Serialize payload → JSON string.
     *   2. INSERT into jobs   (status = PENDING).
     *   3. INSERT into job_outbox (published = false).
     *   Both inserts share the same transaction — if anything fails, both roll back.
     *   4. Return the immutable Job snapshot to the caller.
     *
     * The outbox poller (built later) picks up the unpublished row and pushes
     * {jobId, jobType} to Kafka asynchronously.
     */
    @Override
    @Transactional
    public Job submit(String type, Object payload, int maxAttempts) {
        String json = toJson(payload);

        JobEntity job = JobEntity.newJob(type, json, maxAttempts);
        jobRepository.save(job);

        outboxRepository.save(JobOutboxEntity.of(job.getId(), job.getType()));

        return job.toJob();
    }

    /**
     * Read-only — no @Transactional needed for a single SELECT.
     */
    @Override
    public Job getJob(UUID jobId) {
        return jobRepository.findById(jobId)
                .map(JobEntity::toJob)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    /**
     * Cancel atomically — the JPQL in cancelJob() guards on status = PENDING,
     * so this returns false if the job was already picked up by a worker.
     */
    @Override
    @Transactional
    public boolean cancel(UUID jobId) {
        if (!jobRepository.existsById(jobId)) {
            throw new JobNotFoundException(jobId);
        }
        return jobRepository.cancelJob(jobId, Instant.now()) == 1;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Payload cannot be serialized to JSON", e);
        }
    }
}
