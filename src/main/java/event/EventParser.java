package event;

import java.time.LocalDateTime;
import java.util.Map;

public class EventParser {

    public ChangeEvent parse(
            String tableName,
            OperationType operation,
            Map<String, Object> data,
            String lsn,
            LocalDateTime timestamp
    ) {
        return new ChangeEvent(
                tableName,
                operation,
                data,
                lsn,
                timestamp
        );
    }
}