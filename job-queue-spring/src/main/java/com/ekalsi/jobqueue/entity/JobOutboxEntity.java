package com.ekalsi.jobqueue.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/**
 * One row per job that has been saved to the DB but not yet published to Kafka.
 *
 * Lifecycle:
 *   1. INSERT into jobs + INSERT into job_outbox — in the same DB transaction.
 *   2. Outbox poller reads rows where published = false.
 *   3. Poller publishes {jobId, jobType} to Kafka.
 *   4. Poller flips published = true.
 *
 * This table is internal to the engine. Application code never reads it.
 */
@Entity
@Table(name = "job_outbox")
public class JobOutboxEntity {

    @Id
    private UUID id;

    /**
     * The job this outbox row belongs to.
     * We store just the ID (not a @ManyToOne join) to keep the outbox poller
     * query simple — it only needs jobId and jobType to publish to Kafka.
     */
    @Column(nullable = false)
    private UUID jobId;

    @Column(nullable = false)
    private String jobType;

    @Column(nullable = false)
    private Instant createdAt;

    /**
     * Flipped to true after Kafka acknowledges the publish.
     * The partial index idx_job_outbox_unpublished on (published, created_at)
     * WHERE published = false makes scanning this column extremely fast.
     */
    @Column(nullable = false)
    private boolean published;

    protected JobOutboxEntity() {}

    public static JobOutboxEntity of(UUID jobId, String jobType) {
        JobOutboxEntity e = new JobOutboxEntity();
        e.id        = UUID.randomUUID();
        e.jobId     = jobId;
        e.jobType   = jobType;
        e.createdAt = Instant.now();
        e.published = false;
        return e;
    }

    public UUID getId()          { return id; }
    public UUID getJobId()       { return jobId; }
    public String getJobType()   { return jobType; }
    public Instant getCreatedAt(){ return createdAt; }
    public boolean isPublished() { return published; }

    public void markPublished()  { this.published = true; }
}
