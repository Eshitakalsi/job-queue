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

public interface JobRepository extends JpaRepository<JobEntity, UUID> {

    Page<JobEntity> findByStatus(JobStatus status, Pageable pageable);
    Page<JobEntity> findByType(String type, Pageable pageable);
    Page<JobEntity> findByStatusAndType(JobStatus status, String type, Pageable pageable);

    /**
     * Atomically claims a job. WHERE status = PENDING ensures only one pod wins.
     * Returns 1 on success, 0 if another worker already claimed it.
     */
    @Modifying
    @Query("""
            UPDATE JobEntity j
               SET j.status         = com.ekalsi.jobqueue.JobStatus.IN_PROGRESS,
                   j.leaseOwner     = :leaseOwner,
                   j.leaseExpiresAt = :leaseExpiresAt,
                   j.startedAt      = :now,
                   j.updatedAt      = :now
             WHERE j.id     = :id
               AND j.status = com.ekalsi.jobqueue.JobStatus.PENDING
            """)
    int claimJob(@Param("id") UUID id,
                 @Param("leaseOwner") String leaseOwner,
                 @Param("leaseExpiresAt") Instant leaseExpiresAt,
                 @Param("now") Instant now);

    @Query("""
            SELECT j FROM JobEntity j
             WHERE j.status = com.ekalsi.jobqueue.JobStatus.IN_PROGRESS
               AND j.leaseExpiresAt < :now
            """)
    List<JobEntity> findExpiredLeases(@Param("now") Instant now);

    /** Returns 1 if cancelled, 0 if the job was no longer PENDING. */
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
