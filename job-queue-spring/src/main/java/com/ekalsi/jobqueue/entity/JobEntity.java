package com.ekalsi.jobqueue.entity;

import com.ekalsi.jobqueue.Job;
import com.ekalsi.jobqueue.JobStatus;
import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * JPA entity — the mutable DB row for a job.
 *
 * Why a separate class from Job (the core record)?
 *   Job     = immutable snapshot, returned to callers, lives in job-queue-core, no framework deps.
 *   JobEntity = mutable DB row with JPA annotations, internal to job-queue-spring.
 *
 * Callers never touch JobEntity. They submit via JobQueueClient.submit() which
 * internally creates a JobEntity, persists it, then returns job.toJob() — the clean snapshot.
 */
@Entity
@Table(name = "jobs")
public class JobEntity {

    /**
     * We assign the UUID ourselves (not DB-generated) so we know the jobId
     * before the INSERT round-trip completes. This lets us publish it to
     * Kafka and return it to the caller in the same transaction.
     */
    @Id
    private UUID id;

    @Column(nullable = false)
    private String type;

    /**
     * Raw JSON string stored as PostgreSQL JSONB.
     * JSONB = binary JSON — Postgres can index and query inside it efficiently.
     * We store it as a plain String in Java; the driver handles the conversion.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    /**
     * EnumType.STRING stores the enum name ("PENDING", "IN_PROGRESS", ...) in the column.
     * Never use EnumType.ORDINAL — it stores 0, 1, 2... which breaks the moment
     * you reorder or add values to the enum.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(nullable = false)
    private int maxAttempts;

    /** Exception message from the last failed attempt. Null until the first failure. */
    private String error;

    /**
     * Identity of the worker pod currently holding the lease.
     * Null when no lease is held (status = PENDING, COMPLETED, DEAD, CANCELLED).
     */
    private String leaseOwner;

    /**
     * Wall-clock deadline for the current lease.
     * The stale-job reaper queries: status = IN_PROGRESS AND leaseExpiresAt < NOW()
     * Spring's naming strategy maps leaseExpiresAt → lease_expires_at in the DB.
     */
    private Instant leaseExpiresAt;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    private Instant startedAt;

    private Instant completedAt;

    // ── JPA requires a no-arg constructor ───────────────────────────────────
    protected JobEntity() {}

    // ── Static factory — the only way to create a new entity ────────────────

    /**
     * Build a brand-new entity ready for INSERT.
     * Status starts at PENDING; timestamps are set to now.
     */
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

    // ── Conversion ───────────────────────────────────────────────────────────

    /**
     * Convert to the immutable core record for returning to callers.
     * Lease fields (leaseOwner, leaseExpiresAt) are intentionally excluded —
     * they are internal engine mechanics, not caller-facing state.
     */
    public Job toJob() {
        return new Job(
                id, type, payload, status,
                attempts, maxAttempts, error,
                createdAt, updatedAt, startedAt, completedAt
        );
    }

    // ── Getters ──────────────────────────────────────────────────────────────

    public UUID getId()               { return id; }
    public String getType()           { return type; }
    public String getPayload()        { return payload; }
    public JobStatus getStatus()      { return status; }
    public int getAttempts()          { return attempts; }
    public int getMaxAttempts()       { return maxAttempts; }
    public String getError()          { return error; }
    public String getLeaseOwner()     { return leaseOwner; }
    public Instant getLeaseExpiresAt(){ return leaseExpiresAt; }
    public Instant getCreatedAt()     { return createdAt; }
    public Instant getUpdatedAt()     { return updatedAt; }
    public Instant getStartedAt()     { return startedAt; }
    public Instant getCompletedAt()   { return completedAt; }

    // ── Setters (JPA + engine internals use these) ───────────────────────────

    public void setStatus(JobStatus status)           { this.status = status; }
    public void setAttempts(int attempts)             { this.attempts = attempts; }
    public void setError(String error)                { this.error = error; }
    public void setLeaseOwner(String leaseOwner)      { this.leaseOwner = leaseOwner; }
    public void setLeaseExpiresAt(Instant t)          { this.leaseExpiresAt = t; }
    public void setUpdatedAt(Instant updatedAt)       { this.updatedAt = updatedAt; }
    public void setStartedAt(Instant startedAt)       { this.startedAt = startedAt; }
    public void setCompletedAt(Instant completedAt)   { this.completedAt = completedAt; }
}
