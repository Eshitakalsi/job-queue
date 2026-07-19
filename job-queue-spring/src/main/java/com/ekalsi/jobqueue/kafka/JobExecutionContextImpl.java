package com.ekalsi.jobqueue.kafka;

import com.ekalsi.jobqueue.JobExecutionContext;
import com.ekalsi.jobqueue.entity.JobEntity;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

class JobExecutionContextImpl implements JobExecutionContext {

    private final JobEntity entity;
    private final Duration leaseDuration;
    private final Runnable leaseRenewer;

    JobExecutionContextImpl(JobEntity entity, Duration leaseDuration, Runnable leaseRenewer) {
        this.entity       = entity;
        this.leaseDuration = leaseDuration;
        this.leaseRenewer  = leaseRenewer;
    }

    @Override public UUID   getJobId()         { return entity.getId(); }
    @Override public String getJobType()       { return entity.getType(); }
    @Override public int    getAttemptNumber() { return entity.getAttempts(); }
    @Override public String getPayload()       { return entity.getPayload(); }

    @Override
    public void renewLease() {
        entity.setLeaseExpiresAt(Instant.now().plus(leaseDuration));
        entity.setUpdatedAt(Instant.now());
        leaseRenewer.run();
    }
}
