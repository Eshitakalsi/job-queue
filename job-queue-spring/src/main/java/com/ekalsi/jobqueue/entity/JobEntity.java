package com.ekalsi.jobqueue.entity;

import com.ekalsi.jobqueue.Job;
import com.ekalsi.jobqueue.JobStatus;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "jobs")
public class JobEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String type;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(nullable = false)
    private int maxAttempts;

    private String error;

    /** Identifies the worker pod currently holding the lease. */
    private String leaseOwner;

    /** Stale-job reaper queries: status = IN_PROGRESS AND leaseExpiresAt < NOW(). */
    private Instant leaseExpiresAt;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    private Instant startedAt;
    private Instant completedAt;

    protected JobEntity() {}

    public static JobEntity newJob(String type, String payload, int maxAttempts) {
        JobEntity e = new JobEntity();
        e.id          = UUID.randomUUID();
        e.type        = type;
        e.payload     = payload;
        e.status      = JobStatus.PENDING;
        e.attempts    = 0;
        e.maxAttempts = maxAttempts;
        e.createdAt   = Instant.now();
        e.updatedAt   = Instant.now();
        return e;
    }

    public Job toJob() {
        return new Job(id, type, payload, status, attempts, maxAttempts, error,
                createdAt, updatedAt, startedAt, completedAt);
    }

    public UUID getId()                { return id; }
    public String getType()            { return type; }
    public String getPayload()         { return payload; }
    public JobStatus getStatus()       { return status; }
    public int getAttempts()           { return attempts; }
    public int getMaxAttempts()        { return maxAttempts; }
    public String getError()           { return error; }
    public String getLeaseOwner()      { return leaseOwner; }
    public Instant getLeaseExpiresAt() { return leaseExpiresAt; }
    public Instant getCreatedAt()      { return createdAt; }
    public Instant getUpdatedAt()      { return updatedAt; }
    public Instant getStartedAt()      { return startedAt; }
    public Instant getCompletedAt()    { return completedAt; }

    public void setStatus(JobStatus status)        { this.status = status; }
    public void setAttempts(int attempts)          { this.attempts = attempts; }
    public void setError(String error)             { this.error = error; }
    public void setLeaseOwner(String leaseOwner)   { this.leaseOwner = leaseOwner; }
    public void setLeaseExpiresAt(Instant t)       { this.leaseExpiresAt = t; }
    public void setUpdatedAt(Instant updatedAt)    { this.updatedAt = updatedAt; }
    public void setStartedAt(Instant startedAt)    { this.startedAt = startedAt; }
    public void setCompletedAt(Instant completedAt){ this.completedAt = completedAt; }
}
