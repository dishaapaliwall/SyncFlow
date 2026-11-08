
package com.syncflow.event;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents one complete transaction committed by the source database.
 *
 * All row changes inside this transaction must be applied atomically
 * to the target database.
 */
public class SourceTransaction {

    private final String commitLsn;
    private final String endLsn;
    private final LocalDateTime commitTimestamp;
    private final List<ChangeEvent> events;

    public SourceTransaction(
            String commitLsn,
            String endLsn,
            LocalDateTime commitTimestamp,
            List<ChangeEvent> events
    ) {
        this.commitLsn = Objects.requireNonNull(
                commitLsn, "Commit LSN cannot be null");

        this.endLsn = Objects.requireNonNull(
                endLsn, "End LSN cannot be null");

        this.commitTimestamp = Objects.requireNonNull(
                commitTimestamp, "Commit timestamp cannot be null");

        Objects.requireNonNull(events, "Events cannot be null");

        this.events = Collections.unmodifiableList(
                new ArrayList<>(events)
        );
    }

    public String getCommitLsn() {
        return commitLsn;
    }

    public String getEndLsn() {
        return endLsn;
    }

    public LocalDateTime getCommitTimestamp() {
        return commitTimestamp;
    }

    public List<ChangeEvent> getEvents() {
        return events;
    }
}
