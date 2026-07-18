# Job Queue Engine — PRD

## What Are We Building?

A **reusable Java library** (Spring Boot Starter JAR) that any internal service can drop in as a Maven/Gradle dependency to gain a fully-managed, Kafka-backed job queue. The consuming application registers handlers for job types it cares about; the library handles everything else — storage, Kafka pub/sub, retry, dead-lettering, status tracking, and horizontal scaling.

The library is **not** a standalone service. There is no central queue server. Every app that adds the dependency becomes both a submitter and a worker for the job types it registers.

---

## Core Concepts

| Term | Meaning |
|---|---|
| **Job** | A unit of work with a type, payload, and lifecycle status |
| **Job Type** | A string key that maps to a registered handler (e.g. `SEND_EMAIL`, `GENERATE_REPORT`) |
| **Handler** | A bean in the consuming app that knows how to process one job type |
| **Worker Pod** | A running instance of the consuming app; auto-starts consuming Kafka on startup |
| **Job Status** | `PENDING → IN_PROGRESS → COMPLETED / FAILED / DEAD / CANCELLED` |
| **Lease** | A time-bounded claim on a job; prevents two pods from processing the same job if one pod dies mid-execution |
| **Outbox** | An internal table used to reliably bridge the DB write and the Kafka publish in a single transaction |
| **Stale Job Reaper** | A background thread that detects expired leases and re-queues stuck jobs |

---

## Functional Requirements

1. **Submit a job** — caller provides job type + arbitrary JSON payload; library inserts to DB and enqueues via Kafka atomically (outbox pattern)
2. **Process jobs** — worker pods consume Kafka events, claim a lease, fetch the full job, route to the correct registered handler
3. **Retry on failure** — failed jobs retry up to a configurable max; each retry is a fresh Kafka message so other pods can pick it up
4. **Dead letter** — jobs that exhaust retries move to `DEAD` status for manual inspection; published to a dead-letter Kafka topic
5. **Track status** — any pod can poll job status via the shared Postgres store
6. **Pluggable handlers** — adding a new job type = registering a new `@Component` bean; no library changes
7. **Cancel a job** — cancel a `PENDING` job before it is claimed; returns `409` if already `IN_PROGRESS`
8. **List jobs** — filterable by status, type, and time range with pagination
9. **Stale lease recovery** — if a pod dies while holding a lease, the reaper re-queues the job automatically
10. **At-least-once delivery guarantee** — even if the Kafka publish step crashes, a fallback outbox poller ensures eventual delivery

---

## Non-Functional Requirements

1. **Packaged as a Spring Boot Starter** — one dependency, zero boilerplate; auto-configuration wires everything if beans are present
2. **At-least-once delivery** — no job is silently dropped; the outbox pattern closes the gap between DB write and Kafka publish
3. **Idempotent handler contract** — handlers must tolerate duplicate calls for the same `jobId`; the library surfaces the `jobId` and attempt number to help
4. **Horizontal scaling** — adding more worker pods increases throughput; Kafka partitioning distributes load automatically
5. **Ordered per job type** — jobs of the same type are processed in submission order (partition key = `job_type`)
6. **Lease-protected execution** — a job being processed holds a DB lease; if the pod dies the reaper re-queues it after the lease expires
7. **Isolation between applications** — each application uses its own Kafka consumer group (configurable), so apps don't steal each other's jobs
8. **No schema migrations on the consuming app** — the library ships a Flyway migration that runs on startup
9. **Testable without Kafka** — a test starter (`-test` artifact) provides an in-memory no-op Kafka transport for unit/integration tests

---

## Out of Scope (for now)

- Job scheduling / delayed execution (run at a future time)
- Job priorities
- Job chaining / dependencies
- UI dashboard
- Multi-tenancy job isolation within a single app instance
- Circuit breaker per job type

---

## Data Model

### `jobs` table

```sql
jobs (
  id              UUID PRIMARY KEY,
  type            VARCHAR NOT NULL,           -- e.g. SEND_EMAIL
  payload         JSONB NOT NULL,             -- arbitrary input for the handler
  status          VARCHAR NOT NULL,           -- PENDING | IN_PROGRESS | COMPLETED | FAILED | DEAD | CANCELLED
  attempts        INT NOT NULL DEFAULT 0,
  max_attempts    INT NOT NULL DEFAULT 3,
  error           TEXT,                       -- last failure reason
  lease_owner     VARCHAR,                    -- pod identity that holds the current lease
  lease_expires_at TIMESTAMP,                 -- wall-clock deadline for the current lease
  created_at      TIMESTAMP NOT NULL,
  updated_at      TIMESTAMP NOT NULL,
  started_at      TIMESTAMP,
  completed_at    TIMESTAMP
)
```

### `job_outbox` table (internal — do not query directly)

```sql
job_outbox (
  id          UUID PRIMARY KEY,
  job_id      UUID NOT NULL REFERENCES jobs(id),
  job_type    VARCHAR NOT NULL,
  created_at  TIMESTAMP NOT NULL,
  published   BOOLEAN NOT NULL DEFAULT FALSE  -- flipped to TRUE after Kafka ack
)
```

The `jobs` insert and `job_outbox` insert happen in a single DB transaction. A background outbox poller reads unpublished rows, publishes to Kafka, then marks them published. This eliminates the race between the DB write and the Kafka publish.

---

## Architecture

```
Consuming App (submitter side)
  ↓  jobQueueClient.submit(type, payload)
JobQueueLibrary
  ↓  BEGIN TRANSACTION
  ↓    INSERT jobs (status=PENDING)
  ↓    INSERT job_outbox (published=false)
  ↓  COMMIT
  ↓
Outbox Poller (background thread in library)
  ↓  SELECT unpublished rows FROM job_outbox WHERE published=false
  ↓  publish {job_id, job_type} to Kafka topic: {app}.jobs.created
  ↓  UPDATE job_outbox SET published=true WHERE id=?
  ↓
Kafka (partitioned by job_type, one topic per app or shared with prefix)
  ↓
Worker Pod(s) — consumer group: {app}-job-workers
  ↓  receive {job_id, job_type}
  ↓  BEGIN TRANSACTION
  ↓    SELECT job WHERE id=? AND (status=PENDING OR (status=FAILED AND attempts < max_attempts))
  ↓    UPDATE SET status=IN_PROGRESS, lease_owner=?, lease_expires_at=NOW()+interval
  ↓  COMMIT
  ↓  route to registered handler by job_type
  ↓  handler executes
  ↓  success → UPDATE status=COMPLETED, completed_at=NOW()
     failure → UPDATE status=FAILED, attempts++, error=?
               attempts < max_attempts → re-publish to Kafka (retry)
               attempts >= max_attempts → UPDATE status=DEAD
                                         publish to dead-letter topic

Stale Job Reaper (background thread, runs on all pods, leader-elected via DB lock)
  ↓  SELECT jobs WHERE status=IN_PROGRESS AND lease_expires_at < NOW()
  ↓  UPDATE status=FAILED, lease_owner=NULL
  ↓  re-publish to Kafka for retry
```

---

## Library Module Structure

```
job-queue-engine/
├── job-queue-core/          # domain model, interfaces, JobHandler contract
├── job-queue-spring/        # Spring Boot auto-configuration, Kafka wiring, REST endpoints
├── job-queue-test/          # in-memory transport, test helpers (no Kafka required)
└── job-queue-bom/           # BOM for version alignment
```

### Maven coordinates

```xml
<!-- Core engine — add this -->
<dependency>
  <groupId>com.yourorg</groupId>
  <artifactId>job-queue-spring</artifactId>
  <version>1.0.0</version>
</dependency>

<!-- In test scope only -->
<dependency>
  <groupId>com.yourorg</groupId>
  <artifactId>job-queue-test</artifactId>
  <version>1.0.0</version>
  <scope>test</scope>
</dependency>
```

---

## Handler Contract

```java
public interface JobHandler {
    /**
     * Unique job type this handler processes (matches jobs.type).
     */
    String getJobType();

    /**
     * Execute the job. Called at-least-once — implementations must be idempotent.
     * Throw any exception to signal failure; the engine handles retry/dead-letter.
     *
     * @param context  carries jobId, attempt number, payload, and a lease-renewal callback
     */
    void handle(JobExecutionContext context) throws Exception;
}

public interface JobExecutionContext {
    UUID getJobId();
    String getJobType();
    int getAttemptNumber();       // 1-based; useful for exponential back-off decisions
    JsonNode getPayload();
    void renewLease();            // call periodically for long-running jobs to prevent reaper eviction
}
```

Registering a new job type: create a `@Component` (or `@Bean`) that implements `JobHandler`. The engine auto-discovers all handlers via Spring's application context on startup. No XML, no registry calls.

---

## API

The library exposes a REST controller auto-wired under a configurable base path (default `/jobs`). Apps can disable it and use the `JobQueueClient` Java API directly.

### Submit a job
```
POST /jobs
{
  "type": "SEND_EMAIL",
  "payload": { "to": "user@example.com", "subject": "Hello" },
  "maxAttempts": 3        // optional, defaults to job-queue.defaults.max-attempts
}

Response 201:
{ "jobId": "uuid", "status": "PENDING" }
```

### Get job status
```
GET /jobs/{id}

Response 200:
{
  "jobId": "uuid",
  "type": "SEND_EMAIL",
  "status": "COMPLETED",
  "attempts": 1,
  "createdAt": "...",
  "startedAt": "...",
  "completedAt": "..."
}
```

### Cancel a job
```
DELETE /jobs/{id}    -- only works if status = PENDING

Response 200: { "jobId": "uuid", "status": "CANCELLED" }
Response 409: { "error": "Job is IN_PROGRESS and cannot be cancelled" }
Response 404: { "error": "Job not found" }
```

### List jobs
```
GET /jobs?status=FAILED&type=SEND_EMAIL&from=2024-01-01&page=0&size=20

Response 200:
{
  "content": [ ... ],
  "page": 0,
  "size": 20,
  "totalElements": 47
}
```

---

## Configuration Reference

All properties are under the `job-queue` namespace in `application.yml`.

```yaml
job-queue:
  enabled: true                           # set false to disable the engine entirely (e.g. in batch jobs)
  consumer-group: ${spring.application.name}-job-workers
  kafka:
    topic-prefix: ""                      # prefix prepended to all topic names; use app name to isolate
    jobs-topic: jobs.created              # full name = topic-prefix + jobs-topic
    dead-letter-topic: jobs.dead
    concurrency: 3                        # parallel Kafka listener threads per pod
  outbox:
    poll-interval-ms: 1000               # how often to flush unpublished outbox rows
    batch-size: 100
  lease:
    duration-seconds: 60                 # how long a pod holds a job before the reaper can reclaim it
    renewal-interval-seconds: 15         # how often handle() should call context.renewLease() (guideline)
  reaper:
    enabled: true
    interval-seconds: 30                 # how often the reaper scans for expired leases
  defaults:
    max-attempts: 3
  rest:
    enabled: true
    base-path: /jobs
```

---

## Lease and Stale Job Recovery

### Why a lease?

Kafka commit-offset-on-success is not enough. If a pod fetches a Kafka message, sets `IN_PROGRESS`, then crashes before committing the offset or before finishing the job, the Kafka message may or may not be re-delivered depending on Kafka's `enable.auto.commit` setting. The DB row is stuck at `IN_PROGRESS` indefinitely.

A lease is a `(lease_owner, lease_expires_at)` pair written atomically when the pod claims the job. The reaper scans periodically and re-queues any `IN_PROGRESS` job whose `lease_expires_at` is in the past.

### Lease renewal

For jobs that legitimately take longer than `lease.duration-seconds`, the handler calls `context.renewLease()` to push `lease_expires_at` forward. This is a lightweight `UPDATE` on the jobs row.

### Reaper leader election

All pods run the reaper but only one acts at a time. Leader election is done via a `SELECT ... FOR UPDATE SKIP LOCKED` advisory lock on a dedicated row in Postgres — no ZooKeeper or external coordination required.

---

## Reliability Guarantees Summary

| Scenario | Outcome |
|---|---|
| App crashes after DB write, before Kafka publish | Outbox poller re-publishes on next poll cycle |
| Pod dies after Kafka consume, before job starts | Kafka re-delivers message (no offset commit yet); job stays PENDING |
| Pod dies while job is `IN_PROGRESS` | Reaper detects expired lease; re-queues for retry |
| Handler throws exception | `attempts++`, re-published to Kafka if under `max_attempts` |
| Kafka publish of retry message fails | DB status stays `FAILED`; outbox poller re-publishes on next cycle |
| All retries exhausted | Status → `DEAD`; published to dead-letter Kafka topic |
| Duplicate Kafka delivery (at-least-once) | Handler must be idempotent; `jobId` + `attemptNumber` available for dedup |

---

## Testing Without Kafka

The `job-queue-test` artifact replaces the Kafka transport with a synchronous in-memory dispatcher. Submitting a job immediately invokes the handler in the same thread. No Docker required.

```java
@SpringBootTest
@ActiveProfiles("test")
class SendEmailHandlerTest {

    @Autowired JobQueueClient client;

    @Test
    void sendsEmail() {
        JobSubmission job = client.submit("SEND_EMAIL", Map.of("to", "a@b.com"));
        // handler ran synchronously; job is already COMPLETED
        assertThat(client.getStatus(job.getJobId()).getStatus()).isEqualTo(JobStatus.COMPLETED);
    }
}
```

---

## Observability

The library auto-registers Micrometer metrics if `micrometer-core` is on the classpath:

| Metric | Type | Description |
|---|---|---|
| `job_queue.submitted_total` | Counter | Jobs submitted, tagged by `type` |
| `job_queue.completed_total` | Counter | Jobs completed, tagged by `type` |
| `job_queue.failed_total` | Counter | Jobs failed (not dead), tagged by `type` |
| `job_queue.dead_total` | Counter | Jobs moved to DEAD, tagged by `type` |
| `job_queue.execution_duration_seconds` | Timer | Handler execution time, tagged by `type` |
| `job_queue.queue_depth` | Gauge | Current PENDING jobs per type |
| `job_queue.lease_reclaimed_total` | Counter | Jobs reclaimed by stale job reaper |

A Spring Boot Actuator health indicator (`/actuator/health/jobQueue`) reports DEGRADED if the outbox has unpublished rows older than a configurable threshold (default 5 minutes).

---

## Build Checklist

### Infrastructure
- [ ] Docker Compose: Postgres + Kafka (KRaft, no ZooKeeper)
- [ ] Flyway migrations: `jobs`, `job_outbox` tables

### Core library (`job-queue-core`)
- [ ] `Job` domain object + `JobStatus` enum
- [ ] `JobHandler` interface + `JobExecutionContext` interface
- [ ] `JobQueueClient` interface (submit, getStatus, cancel, list)

### Spring module (`job-queue-spring`)
- [ ] Spring Boot auto-configuration class
- [ ] `JobRepository` (Spring Data JPA)
- [ ] `POST /jobs` — transactional insert to `jobs` + `job_outbox`
- [ ] `GET /jobs/{id}` — status polling
- [ ] `DELETE /jobs/{id}` — cancel (PENDING only)
- [ ] `GET /jobs` — list with filters + pagination
- [ ] Outbox poller (scheduled background thread)
- [ ] Kafka producer (publish job events)
- [ ] Kafka consumer (partitioned by job type, configurable concurrency)
- [ ] Job claim logic — atomic `SELECT + UPDATE` with lease
- [ ] Handler registry — auto-discover all `JobHandler` beans on startup
- [ ] Retry logic — re-publish to Kafka on failure; DEAD after `max_attempts`
- [ ] Dead-letter publisher
- [ ] Stale job reaper — scheduled, leader-elected via `SELECT FOR UPDATE SKIP LOCKED`
- [ ] Lease renewal via `context.renewLease()`
- [ ] Micrometer metrics
- [ ] Actuator health indicator
- [ ] Configuration properties class (`@ConfigurationProperties("job-queue")`)

### Test module (`job-queue-test`)
- [ ] In-memory synchronous transport (no Kafka required)
- [ ] Test auto-configuration that replaces Kafka beans

### Sample handlers (in a `job-queue-sample` module, not shipped)
- [ ] `LogJobHandler` — logs payload, always succeeds
- [ ] `FailingJobHandler` — always throws, validates retry + dead-letter path
- [ ] `SlowJobHandler` — sleeps 90s, validates lease renewal path

### BOM
- [ ] `job-queue-bom` for version alignment across modules
