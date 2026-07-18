package com.ekalsi.jobqueue.repository;

import com.ekalsi.jobqueue.entity.JobOutboxEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface JobOutboxRepository extends JpaRepository<JobOutboxEntity, UUID> {

    /**
     * Fetch the oldest unpublished rows up to a configurable batch size.
     * The outbox poller calls this on every tick, publishes each to Kafka,
     * then calls markPublished() below.
     *
     * Pageable controls the LIMIT — caller passes PageRequest.of(0, batchSize).
     * Ordering by createdAt ensures jobs are published in submission order.
     */
    @Query("SELECT o FROM JobOutboxEntity o WHERE o.published = false ORDER BY o.createdAt ASC")
    List<JobOutboxEntity> findUnpublished(Pageable pageable);

    /**
     * Flip published = true for a batch of rows in one UPDATE instead of N individual saves.
     * Called after the Kafka producer successfully acknowledges the publish.
     */
    @Modifying
    @Query("UPDATE JobOutboxEntity o SET o.published = true WHERE o.id IN :ids")
    void markPublished(@Param("ids") List<UUID> ids);
}
