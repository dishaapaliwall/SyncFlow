package reliability;

import event.ChangeEvent;

import java.util.HashSet;
import java.util.Set;

public class DeduplicationManager {

    private final Set<String> processedEvents = new HashSet<>();

    public boolean isDuplicate(ChangeEvent event) {

        if (event == null) {
            throw new IllegalArgumentException("Event cannot be null");
        }

        String eventId = createEventId(event);

        if (processedEvents.contains(eventId)) {
            return true;
        }

        processedEvents.add(eventId);
        return false;
    }

    private String createEventId(ChangeEvent event) {

        return event.getLsn()
                + "-"
                + event.getTableName()
                + "-"
                + event.getOperation();
    }
}