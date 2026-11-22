package com.syncflow.snapshot.validation;

import java.util.Collections;
import java.util.List;

public class PreflightValidationException extends Exception {

    private final List<String> errors;
    private final PreflightReport report;

    public PreflightValidationException(String message, List<String> errors, PreflightReport report) {
        super(message);
        this.errors = errors != null ? List.copyOf(errors) : Collections.emptyList();
        this.report = report;
    }

    public List<String> getErrors() {
        return errors;
    }

    public PreflightReport getReport() {
        return report;
    }
}
