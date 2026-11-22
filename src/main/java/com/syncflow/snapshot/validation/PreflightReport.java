package com.syncflow.snapshot.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PreflightReport {

    public record CheckResult(
            String checkName,
            boolean passed,
            String details
    ) {}

    private final List<CheckResult> checks = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final List<String> errors = new ArrayList<>();

    public void addCheck(String name, boolean passed, String details) {
        checks.add(new CheckResult(name, passed, details));
        if (!passed && details != null) {
            errors.add("[" + name + "] " + details);
        }
    }

    public void addWarning(String warning) {
        if (warning != null && !warning.isBlank()) {
            warnings.add(warning);
        }
    }

    public void addError(String name, String errorMessage) {
        addCheck(name, false, errorMessage);
    }

    public boolean isSuccess() {
        return errors.isEmpty();
    }

    public List<CheckResult> getChecks() {
        return Collections.unmodifiableList(checks);
    }

    public List<String> getWarnings() {
        return Collections.unmodifiableList(warnings);
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public void logReport() {
        System.out.println("========== BOOTSTRAP PREFLIGHT VALIDATION REPORT ==========");
        for (CheckResult check : checks) {
            String status = check.passed() ? "PASSED" : "FAILED";
            System.out.printf("[%s] %s: %s%n", status, check.checkName(), check.details());
        }
        if (!warnings.isEmpty()) {
            System.out.println("--- WARNINGS & RISKS ---");
            for (String w : warnings) {
                System.out.println("  [WARN] " + w);
            }
        }
        if (!errors.isEmpty()) {
            System.out.println("--- ACTIONABLE FAILURES ---");
            for (String e : errors) {
                System.out.println("  [ERROR] " + e);
            }
        }
        System.out.println("Overall Result: " + (isSuccess() ? "SUCCESS" : "FAILED"));
        System.out.println("============================================================");
    }
}
