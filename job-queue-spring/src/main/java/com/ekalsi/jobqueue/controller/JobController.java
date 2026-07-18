package com.ekalsi.jobqueue.controller;

import com.ekalsi.jobqueue.Job;
import com.ekalsi.jobqueue.JobNotFoundException;
import com.ekalsi.jobqueue.JobQueueClient;
import com.ekalsi.jobqueue.JobStatus;
import com.ekalsi.jobqueue.service.JobQueryService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobQueueClient jobQueueClient;
    private final JobQueryService jobQueryService;

    public JobController(JobQueueClient jobQueueClient, JobQueryService jobQueryService) {
        this.jobQueueClient   = jobQueueClient;
        this.jobQueryService  = jobQueryService;
    }

    /**
     * POST /jobs
     * Submit a new job. Returns 201 Created with the job snapshot.
     *
     * Body: { "type": "SEND_EMAIL", "payload": { ... }, "maxAttempts": 3 }
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Job submit(@RequestBody JobRequest request) {
        if (request.maxAttempts() != null) {
            return jobQueueClient.submit(request.type(), request.payload(), request.maxAttempts());
        }
        return jobQueueClient.submit(request.type(), request.payload());
    }

    /**
     * GET /jobs/{id}
     * Fetch the current state of a job. Returns 404 if the job doesn't exist.
     */
    @GetMapping("/{id}")
    public Job getJob(@PathVariable UUID id) {
        return jobQueueClient.getJob(id);
    }

    /**
     * DELETE /jobs/{id}
     * Cancel a PENDING job.
     * Returns 200 on success, 409 if the job is already IN_PROGRESS or beyond.
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable UUID id) {
        boolean cancelled = jobQueueClient.cancel(id);
        if (!cancelled) {
            return ResponseEntity
                    .status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Job is already in progress or completed"));
        }
        return ResponseEntity.ok(Map.of("jobId", id, "status", "CANCELLED"));
    }

    /**
     * GET /jobs?status=PENDING&type=SEND_EMAIL&page=0&size=20
     * List jobs with optional filters. Supports Spring Data pagination params.
     *
     * @PageableDefault sets the fallback if the caller doesn't pass page/size.
     */
    @GetMapping
    public Page<Job> listJobs(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) String type,
            @PageableDefault(size = 20) Pageable pageable) {
        return jobQueryService.listJobs(status, type, pageable);
    }
}
