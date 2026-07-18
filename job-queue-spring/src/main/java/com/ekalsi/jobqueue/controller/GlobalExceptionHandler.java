package com.ekalsi.jobqueue.controller;

import com.ekalsi.jobqueue.JobNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * Translates domain exceptions into HTTP responses.
 * Without this, Spring returns a generic 500 for any unhandled exception.
 *
 * @RestControllerAdvice = @ControllerAdvice + @ResponseBody
 * It intercepts exceptions thrown from any @RestController and lets us
 * return a structured JSON error body instead of an HTML error page.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * JobNotFoundException → 404 Not Found
     */
    @ExceptionHandler(JobNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleNotFound(JobNotFoundException ex) {
        return Map.of("error", ex.getMessage());
    }

    /**
     * IllegalArgumentException → 400 Bad Request
     * Covers non-serializable payloads and any other bad caller input.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleBadRequest(IllegalArgumentException ex) {
        return Map.of("error", ex.getMessage());
    }

    /** Catch-all — exposes the real exception during development. Remove before prod. */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Map<String, String> handleAll(Exception ex) {
        ex.printStackTrace();
        return Map.of("error", ex.getClass().getName(), "message", String.valueOf(ex.getMessage()));
    }
}
