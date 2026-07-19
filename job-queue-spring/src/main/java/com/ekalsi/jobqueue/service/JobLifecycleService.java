package com.ekalsi.jobqueue.service;

import com.ekalsi.jobqueue.JobStatus;
import com.ekalsi.jobqueue.entity.JobEntity;
import com.ekalsi.jobqueue.entity.JobOutboxEntity;
import com.ekalsi.jobqueue.repository.JobOutboxRepository;
import com.ekalsi.jobqueue.repository.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Handles job state transitions invoked from the Kafka worker.
 * Methods are @Transactional so @Modifying queries work — @KafkaListener
 * methods don't get a Spring transaction automatically.
 */
@Service
public class JobLifecycleService {

    private final JobRepository jobRepository;
    private final JobOutboxRepository outboxRepository;

    public JobLifecycleService(JobRepository jobRepository,
                               JobOutboxRepository outboxRepository) {
        this.jobRepository    = jobRepository;
        this.outboxRepository = outboxRepository;
    }

    /** Claims the job and returns it with attempts already incremented. Empty if another worker got there first. */
    @Transactional
    public Optional<JobEntity> claimAndFetch(UUID jobId, String leaseOwner, Instant leaseExpiresAt) {
        int claimed = jobRepository.claimJob(jobId, leaseOwner, leaseExpiresAt, Instant.now());
        if (claimed == 0) return Optional.empty();

        JobEntity job = jobRepository.findById(jobId).orElse(null);
        if (job == null) return Optional.empty();

        job.setAttempts(job.getAttempts() + 1);
        jobRepository.save(job);
        return Optional.of(job);
    }

    @Transactional
    public void renewLease(JobEntity job, Duration leaseTtl) {
        job.setLeaseExpiresAt(Instant.now().plus(leaseTtl));
        job.setUpdatedAt(Instant.now());
        jobRepository.save(job);
    }

    @Transactional
    public void markCompleted(JobEntity job) {
        Instant now = Instant.now();
        job.setStatus(JobStatus.COMPLETED);
        job.setCompletedAt(now);
        job.setUpdatedAt(now);
        job.setLeaseOwner(null);
        job.setLeaseExpiresAt(null);
        jobRepository.save(job);
    }

    /** Resets to PENDING and writes a new outbox row so the poller re-publishes to Kafka. */
    @Transactional
    public void scheduleRetry(JobEntity job, String error) {
        Instant now = Instant.now();
        job.setStatus(JobStatus.PENDING);
        job.setError(error);
        job.setUpdatedAt(now);
        job.setLeaseOwner(null);
        job.setLeaseExpiresAt(null);
        jobRepository.save(job);
        outboxRepository.save(JobOutboxEntity.of(job.getId(), job.getType()));
    }

    @Transactional
    public void markOutboxPublished(List<UUID> ids) {
        outboxRepository.markPublished(ids);
    }

    @Transactional
    public void markDead(JobEntity job, String error) {
        Instant now = Instant.now();
        job.setStatus(JobStatus.DEAD);
        job.setError(error);
        job.setCompletedAt(now);
        job.setUpdatedAt(now);
        job.setLeaseOwner(null);
        job.setLeaseExpiresAt(null);
        jobRepository.save(job);
    }
}
