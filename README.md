# job-queue

A Kafka-backed, Postgres-persisted job queue engine distributed as a Spring Boot library. Add it as a Maven/Gradle dependency, create two tables, implement one interface — your service has a durable, retryable background job queue.

**This is a library, not a deployable service.** Jobs are submitted and queried via injected Java interfaces.

## How it works


```
submit()
  └─ INSERT jobs + job_outbox (one DB transaction)
       └─ Postgres WAL → Debezium CDC
            └─ job-queue-db.public.job_outbox (Kafka topic)
                 └─ OutboxCdcListener
                      └─ KafkaTemplate.send(job-queue.jobs)
                           └─ @KafkaListener (JobWorker)
                                └─ atomic UPDATE WHERE status = PENDING  ← mutual exclusion
                                     └─ JobHandler.handle(ctx)
                                          ├─ success  → COMPLETED
                                          ├─ failure, attempts < max → PENDING + new outbox row (retry)
                                          └─ failure, attempts = max → DEAD
```

---

## Prerequisites

- Java 21
- Spring Boot 4.1+
- PostgreSQL 16+
- Kafka (KRaft mode, no ZooKeeper required)
