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

    @Query("SELECT o FROM JobOutboxEntity o WHERE o.published = false ORDER BY o.createdAt ASC")
    List<JobOutboxEntity> findUnpublished(Pageable pageable);

    @Modifying
    @Query("UPDATE JobOutboxEntity o SET o.published = true WHERE o.id IN :ids")
    void markPublished(@Param("ids") List<UUID> ids);
}
