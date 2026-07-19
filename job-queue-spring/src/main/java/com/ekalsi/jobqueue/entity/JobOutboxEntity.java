package com.ekalsi.jobqueue.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_outbox")
public class JobOutboxEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID jobId;

    @Column(nullable = false)
    private String jobType;

    @Column(nullable = false)
    private Instant createdAt;

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

    public UUID getId()           { return id; }
    public UUID getJobId()        { return jobId; }
    public String getJobType()    { return jobType; }
    public Instant getCreatedAt() { return createdAt; }
    public boolean isPublished()  { return published; }

    public void markPublished()   { this.published = true; }
}
