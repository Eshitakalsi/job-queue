package com.ekalsi.jobqueue.kafka;

import com.ekalsi.jobqueue.config.JobQueueProperties;
import com.ekalsi.jobqueue.kafka.cdc.DebeziumOutboxEvent;
import com.ekalsi.jobqueue.service.JobLifecycleService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
public class OutboxCdcListener {

    private static final Logger log = LoggerFactory.getLogger(OutboxCdcListener.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JobQueueProperties properties;
    private final ObjectMapper objectMapper;
    private final JobLifecycleService lifecycleService;

    public OutboxCdcListener(KafkaTemplate<String, String> kafkaTemplate,
                             JobQueueProperties properties,
                             ObjectMapper objectMapper,
                             JobLifecycleService lifecycleService) {
        this.kafkaTemplate    = kafkaTemplate;
        this.properties       = properties;
        this.objectMapper     = objectMapper;
        this.lifecycleService = lifecycleService;
    }

    @KafkaListener(
            topics          = "${job-queue.cdc.outbox-topic:job-queue-db.public.job_outbox}",
            groupId         = "${job-queue.consumer-group:job-queue-workers}-cdc",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onOutboxChange(String message) throws Exception {
        DebeziumOutboxEvent event = objectMapper.readValue(message, DebeziumOutboxEvent.class);

        if (!"c".equals(event.payload().op())) return;

        var after = event.payload().after();
        if (after == null) return;

        String topic   = properties.kafka().resolvedJobsTopic();
        String payload = objectMapper.writeValueAsString(new JobMessage(after.jobId(), after.jobType()));

        kafkaTemplate.send(topic, after.jobId().toString(), payload).get();
        lifecycleService.markOutboxPublished(List.of(after.id()));

        log.info("CDC: published job {} ({}) to '{}'", after.jobId(), after.jobType(), topic);
    }
}
