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
        this.jobRepository    = jobRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper     = objectMapper;
        this.properties       = properties;
    }

    @Override
    @Transactional
    public Job submit(String type, Object payload) {
        return submit(type, payload, properties.defaults().maxAttempts());
    }

    @Override
    @Transactional
    public Job submit(String type, Object payload, int maxAttempts) {
        JobEntity job = JobEntity.newJob(type, toJson(payload), maxAttempts);
        jobRepository.save(job);
        outboxRepository.save(JobOutboxEntity.of(job.getId(), job.getType()));
        return job.toJob();
    }

    @Override
    public Job getJob(UUID jobId) {
        return jobRepository.findById(jobId)
                .map(JobEntity::toJob)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    @Override
    @Transactional
    public boolean cancel(UUID jobId) {
        if (!jobRepository.existsById(jobId)) {
            throw new JobNotFoundException(jobId);
        }
        return jobRepository.cancelJob(jobId, Instant.now()) == 1;
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Payload cannot be serialized to JSON", e);
        }
    }
}
