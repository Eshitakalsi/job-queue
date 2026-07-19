package com.ekalsi.jobqueue.kafka;

import com.ekalsi.jobqueue.config.JobQueueProperties;
import com.ekalsi.jobqueue.repository.JobOutboxRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final JobOutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JobQueueProperties properties;
    private final ObjectMapper objectMapper;

    public OutboxPoller(JobOutboxRepository outboxRepository,
                        KafkaTemplate<String, String> kafkaTemplate,
                        JobQueueProperties properties,
                        ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate    = kafkaTemplate;
        this.properties       = properties;
        this.objectMapper     = objectMapper;
    }

    @Scheduled(fixedDelayString = "${job-queue.outbox.poll-interval-ms:1000}")
    @Transactional
    public void poll() {
        var batch = outboxRepository.findUnpublished(
                PageRequest.of(0, properties.outbox().batchSize()));

        if (batch.isEmpty()) return;

        String topic = properties.kafka().resolvedJobsTopic();
        List<UUID> published = new ArrayList<>();

        for (var row : batch) {
            try {
                kafkaTemplate.send(topic, row.getJobId().toString(),
                        serialize(new JobMessage(row.getJobId(), row.getJobType()))).get();
                published.add(row.getId());
            } catch (Exception e) {
                log.warn("Failed to publish job {} to Kafka, will retry next tick", row.getJobId(), e);
                break;
            }
        }

        if (!published.isEmpty()) {
            outboxRepository.markPublished(published);
            log.info("Flushed {} job(s) to topic '{}'", published.size(), topic);
        }
    }

    private String serialize(JobMessage msg) throws JsonProcessingException {
        return objectMapper.writeValueAsString(msg);
    }
}
