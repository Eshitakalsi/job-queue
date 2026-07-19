package com.ekalsi.jobqueue.kafka;

import com.ekalsi.jobqueue.JobHandler;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> handlers;

    public HandlerRegistry(List<JobHandler> allHandlers) {
        this.handlers = allHandlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        JobHandler::getJobType,
                        h -> h,
                        (a, b) -> { throw new IllegalStateException(
                                "Duplicate JobHandler for type: " + a.getJobType()); }
                ));
    }

    public Optional<JobHandler> find(String jobType) {
        return Optional.ofNullable(handlers.get(jobType));
    }
}
