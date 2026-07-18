package com.ekalsi.jobqueue.repository;

import com.ekalsi.jobqueue.JobStatus;
import com.ekalsi.jobqueue.entity.JobEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Spring Data JPA repository for JobEntity.
 *
 * Extending JpaRepository<JobEntity, UUID> gives us for free:
 *   save(), findById(), findAll(), delete(), count(), existsById() ...
 *
 * Spring reads the method names below and generates the SQL automatically.
 * The two @Query methods need custom JPQL because they do something
 * Spring can't derive from a method name alone.
 */
public interface JobRepository extends JpaRepository<JobEntity, UUID> {

    // ── Used by the list endpoint ─────────────────────────────────────────────

    // Spring translates this to: SELECT * FROM jobs WHERE status = ?
    Page<JobEntity> findByStatus(JobStatus status, Pageable pageable);

    // SELECT * FROM jobs WHERE type = ?
    Page<JobEntity> findByType(String type, Pageable pageable);

    // SELECT * FROM jobs WHERE status = ? AND type = ?
    Page<JobEntity> findByStatusAndType(JobStatus status, String type, Pageable pageable);

    // ── Atomic job claim — the core of mutual exclusion ──────────────────────
    /**
     * A worker pod calls this when it picks up a Kafka message.
     * The WHERE clause guards on status = PENDING, so only one pod can succeed.
     *
     * Returns: 1 if this pod won the claim, 0 if another pod got there first.
     *
     * This is JPQL (Java Persistence Query Language), not SQL —
     * it references the entity class (JobEntity) and field names, not table/column names.
     * Hibernate translates it to the correct SQL for the target DB.
     */
    @Modifying
    @Query("""
            UPDATE JobEntity j
               SET j.status        = com.ekalsi.jobqueue.JobStatus.IN_PROGRESS,
                   j.leaseOwner    = :leaseOwner,
                   j.leaseExpiresAt = :leaseExpiresAt,
                   j.startedAt     = :now,
                   j.updatedAt     = :now
             WHERE j.id     = :id
               AND j.status = com.ekalsi.jobqueue.JobStatus.PENDING
            """)
    int claimJob(@Param("id") UUID id,
                 @Param("leaseOwner") String leaseOwner,
                 @Param("leaseExpiresAt") Instant leaseExpiresAt,
                 @Param("now") Instant now);

    // ── Used by the stale-job reaper ─────────────────────────────────────────
    /**
     * Find all IN_PROGRESS jobs whose lease has expired.
     * The reaper calls this periodically and re-queues everything it finds.
     */
    @Query("""
            SELECT j FROM JobEntity j
             WHERE j.status = com.ekalsi.jobqueue.JobStatus.IN_PROGRESS
               AND j.leaseExpiresAt < :now
            """)
    List<JobEntity> findExpiredLeases(@Param("now") Instant now);

    // ── Used by the cancel endpoint ───────────────────────────────────────────
    /**
     * Atomically cancel a job — only succeeds if it is still PENDING.
     * Returns 1 on success, 0 if the job was already picked up.
     */
    @Modifying
    @Query("""
            UPDATE JobEntity j
               SET j.status      = com.ekalsi.jobqueue.JobStatus.CANCELLED,
                   j.completedAt = :now,
                   j.updatedAt   = :now
             WHERE j.id     = :id
               AND j.status = com.ekalsi.jobqueue.JobStatus.PENDING
            """)
    int cancelJob(@Param("id") UUID id, @Param("now") Instant now);
}
