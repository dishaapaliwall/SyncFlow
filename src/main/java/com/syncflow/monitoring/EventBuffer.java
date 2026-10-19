
package com.syncflow.monitoring;

import com.syncflow.event.ChangeEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class EventBuffer {

    private final int capacity;
    private final Deque<ChangeEvent> events = new ArrayDeque<>();

    public EventBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException(
                    "Event buffer capacity must be positive"
            );
        }

        this.capacity = capacity;
    }

    public synchronized void add(ChangeEvent event) {
        if (event == null) {
            throw new IllegalArgumentException(
                    "Event cannot be null"
            );
        }

        if (events.size() == capacity) {
            events.removeFirst();
        }

        events.addLast(event);
    }

    public synchronized List<ChangeEvent> getRecentEvents() {
        return new ArrayList<>(events);
    }

    public synchronized int size() {
        return events.size();
    }
}
