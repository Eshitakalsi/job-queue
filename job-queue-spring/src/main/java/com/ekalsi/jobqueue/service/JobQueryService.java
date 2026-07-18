package com.ekalsi.jobqueue.service;

import com.ekalsi.jobqueue.Job;
import com.ekalsi.jobqueue.JobStatus;
import com.ekalsi.jobqueue.entity.JobEntity;
import com.ekalsi.jobqueue.repository.JobRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

/**
 * Read-only query operations that don't belong on the core JobQueueClient interface
 * (which would force a Spring Data dependency into job-queue-core).
 */
@Service
public class JobQueryService {

    private final JobRepository jobRepository;

    public JobQueryService(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public Page<Job> listJobs(JobStatus status, String type, Pageable pageable) {
        Page<com.ekalsi.jobqueue.entity.JobEntity> page;

        if (status != null && type != null) {
            page = jobRepository.findByStatusAndType(status, type, pageable);
        } else if (status != null) {
            page = jobRepository.findByStatus(status, pageable);
        } else if (type != null) {
            page = jobRepository.findByType(type, pageable);
        } else {
            page = jobRepository.findAll(pageable);
        }

        return page.map(JobEntity::toJob);
    }
}
