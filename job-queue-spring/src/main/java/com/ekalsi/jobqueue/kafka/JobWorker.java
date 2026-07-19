package com.ekalsi.jobqueue.kafka;

import com.ekalsi.jobqueue.config.JobQueueProperties;
import com.ekalsi.jobqueue.entity.JobEntity;
import com.ekalsi.jobqueue.service.JobLifecycleService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;

@Component
public class JobWorker {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private final HandlerRegistry     handlerRegistry;
    private final JobLifecycleService lifecycleService;
    private final JobQueueProperties  properties;
    private final ObjectMapper        objectMapper;

    public JobWorker(HandlerRegistry handlerRegistry,
                     JobLifecycleService lifecycleService,
                     JobQueueProperties properties,
                     ObjectMapper objectMapper) {
        this.handlerRegistry  = handlerRegistry;
        this.lifecycleService = lifecycleService;
        this.properties       = properties;
        this.objectMapper     = objectMapper;
    }

    @KafkaListener(
            topics      = "${job-queue.kafka.jobs-topic:jobs.created}",
            groupId     = "${job-queue.consumer-group:job-queue-workers}",
            concurrency = "${job-queue.kafka.concurrency:3}"
    )
    public void consume(ConsumerRecord<String, String> record) {
        JobMessage msg;
        try {
            msg = objectMapper.readValue(record.value(), JobMessage.class);
        } catch (Exception e) {
            log.error("Unparseable message on topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), record.value(), e);
            return;
        }

        Duration leaseTtl = Duration.ofSeconds(properties.lease().durationSeconds());

        var jobOpt = lifecycleService.claimAndFetch(msg.jobId(), buildLeaseOwner(), Instant.now().plus(leaseTtl));
        if (jobOpt.isEmpty()) {
            log.debug("Job {} already claimed — skipping", msg.jobId());
            return;
        }
        JobEntity job = jobOpt.get();

        var handler = handlerRegistry.find(msg.jobType());
        if (handler.isEmpty()) {
            log.warn("No handler for type '{}' — marking job {} DEAD", msg.jobType(), job.getId());
            lifecycleService.markDead(job, "No handler registered for type: " + msg.jobType());
            return;
        }

        var ctx = new JobExecutionContextImpl(job, leaseTtl,
                () -> lifecycleService.renewLease(job, leaseTtl));

        try {
            handler.get().handle(ctx);
            lifecycleService.markCompleted(job);
            log.info("Job {} ({}) completed — attempt {}/{}", job.getId(), job.getType(), job.getAttempts(), job.getMaxAttempts());
        } catch (Exception e) {
            log.warn("Job {} ({}) failed on attempt {}/{}: {}", job.getId(), job.getType(), job.getAttempts(), job.getMaxAttempts(), e.getMessage());
            if (job.getAttempts() >= job.getMaxAttempts()) {
                lifecycleService.markDead(job, e.getMessage());
            } else {
                lifecycleService.scheduleRetry(job, e.getMessage());
            }
        }
    }

    private static String buildLeaseOwner() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + Thread.currentThread().threadId();
        } catch (Exception e) {
            return "unknown-" + Thread.currentThread().threadId();
        }
    }
}
