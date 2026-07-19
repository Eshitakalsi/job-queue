# job-queue

A Kafka-backed, Postgres-persisted job queue engine distributed as a Spring Boot library. Add it as a Maven/Gradle dependency, create two tables, implement one interface — your service has a durable, retryable background job queue.

**This is a library, not a deployable service.** Jobs are submitted and queried via injected Java interfaces.

## How it works

```
submit()
  └─ INSERT jobs + job_outbox (one DB transaction)
       └─ OutboxPoller (every 1s)
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

---

## 1. Install the library

The library is not yet published to Maven Central. Install it to your local repository first:

```bash
git clone https://github.com/Eshitakalsi/job-queue.git
cd job-queue
./mvnw install -DskipTests
```

Then add to your service:

**Maven**
```xml
<dependency>
    <groupId>com.ekalsi</groupId>
    <artifactId>job-queue-spring</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

**Gradle**
```groovy
implementation 'com.ekalsi:job-queue-spring:0.0.1-SNAPSHOT'
```

---

## 2. Create the database tables

```sql
CREATE TABLE jobs (
    id               UUID          PRIMARY KEY,
    type             VARCHAR(255)  NOT NULL,
    payload          JSONB         NOT NULL,
    status           VARCHAR(50)   NOT NULL DEFAULT 'PENDING',
    attempts         INT           NOT NULL DEFAULT 0,
    max_attempts     INT           NOT NULL DEFAULT 3,
    error            TEXT,
    lease_owner      VARCHAR(255),
    lease_expires_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ   NOT NULL,
    updated_at       TIMESTAMPTZ   NOT NULL,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ
);

CREATE TABLE job_outbox (
    id          UUID          PRIMARY KEY,
    job_id      UUID          NOT NULL,
    job_type    VARCHAR(255)  NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL,
    published   BOOLEAN       NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_job_outbox_unpublished ON job_outbox (created_at) WHERE published = false;
```

---

## 3. Create the Kafka topic

```bash
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic job-queue.jobs \
  --partitions 3 \
  --replication-factor 1
```

---

## 4. Configure your application

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/yourdb
spring.datasource.username=youruser
spring.datasource.password=yourpassword
spring.kafka.bootstrap-servers=localhost:9092

spring.jpa.hibernate.ddl-auto=validate

job-queue.kafka.jobs-topic=job-queue.jobs
```

---

## 5. Implement a handler

```java
@Component
public class SendEmailHandler implements JobHandler {

    @Override
    public String getJobType() {
        return "SEND_EMAIL";
    }

    @Override
    public void handle(JobExecutionContext ctx) throws Exception {
        EmailPayload p = objectMapper.readValue(ctx.getPayload(), EmailPayload.class);
        emailService.send(p.to(), p.subject());
    }
}
```

Handlers are auto-discovered on startup. The `type` string in `getJobType()` must exactly match what you pass to `submit()`.

**Handlers must be idempotent.** The engine guarantees at-least-once delivery — `handle()` may be called more than once for the same job. Check before acting if the action has side effects (e.g. confirm the email wasn't already sent).

For long-running handlers, call `ctx.renewLease()` periodically to prevent the stale-job reaper from reclaiming the job mid-execution.

---

## 6. Usage

Inject `JobQueueClient` to submit and manage jobs:

```java
@Service
public class OrderService {

    private final JobQueueClient jobQueue;

    public void placeOrder(Order order) {
        // Submit with default max-attempts (configured via job-queue.defaults.max-attempts)
        Job job = jobQueue.submit("SEND_EMAIL", Map.of(
            "to",      order.customerEmail(),
            "subject", "Your order #" + order.id()
        ));

        // Or override retry limit per job
        jobQueue.submit("GENERATE_REPORT", payload, 5);
    }

    public JobStatus checkStatus(UUID jobId) {
        return jobQueue.getJob(jobId).status();
    }

    public void cancelJob(UUID jobId) {
        boolean cancelled = jobQueue.cancel(jobId);
        // false means the job was already picked up by a worker
    }
}
```

Inject `JobQueryService` to list and filter jobs:

```java
@Autowired
private JobQueryService jobQueryService;

// List pending jobs of a specific type, paginated
Page<Job> jobs = jobQueryService.listJobs(JobStatus.PENDING, "SEND_EMAIL",
    PageRequest.of(0, 20, Sort.by("createdAt").descending()));
```

---

## Job status lifecycle

```
PENDING → IN_PROGRESS → COMPLETED
                      ↘ PENDING   (retry, attempts < max)
                      ↘ DEAD      (attempts exhausted)
PENDING → CANCELLED   (via jobQueue.cancel(id))
```

---

## Configuration reference

All properties under `job-queue.*`. Every property has a default and is optional.

| Property                  | Default             | Description                                                    |
|---------------------------|---------------------|----------------------------------------------------------------|
| `consumer-group`          | `job-queue-workers` | Kafka consumer group. All pods of the same service share one. |
| `kafka.jobs-topic`        | `jobs.created`      | Topic the outbox poller writes to.                             |
| `kafka.topic-prefix`      | *(empty)*           | Prepended to all topic names: `payments` → `payments.jobs.created` |
| `kafka.dead-letter-topic` | `jobs.dead`         | Topic for jobs that exhaust all retries.                       |
| `kafka.concurrency`       | `3`                 | Listener threads per pod. Should not exceed partition count.   |
| `outbox.poll-interval-ms` | `1000`              | How often the poller flushes unpublished rows to Kafka (ms).  |
| `outbox.batch-size`       | `100`               | Max rows published per poll tick.                              |
| `lease.duration-seconds`  | `60`                | Seconds a worker holds a job before the reaper can reclaim it. |
| `reaper.interval-seconds` | `30`                | How often the reaper scans for expired leases.                 |
| `defaults.max-attempts`   | `3`                 | Default retry cap when not specified at submission.            |

---

## Running infrastructure locally

A `docker-compose.yml` is included at the root of the repo. It starts Postgres 16 and Kafka (KRaft mode, no ZooKeeper).

```bash
cp .env.example .env   # fill in your credentials
docker compose up -d
```
