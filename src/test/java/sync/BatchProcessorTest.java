package sync;

import event.ChangeEvent;
import event.OperationType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class BatchProcessorTest {

    @Test
    void shouldProcessBatch() throws Exception {

        TargetWriter targetWriter = mock(TargetWriter.class);
        TransactionManager transactionManager = mock(TransactionManager.class);

        BatchProcessor processor =
                new BatchProcessor(targetWriter, transactionManager);

        List<ChangeEvent> events = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            events.add(createEvent(i));
        }

        processor.process(events);

        verify(transactionManager).begin();
        verify(targetWriter, times(10)).write(any(ChangeEvent.class));
        verify(transactionManager).commit();
        verify(transactionManager, never()).rollback();
    }

    @Test
    void shouldRejectMoreThanTenEvents() {

        TargetWriter targetWriter = mock(TargetWriter.class);
        TransactionManager transactionManager = mock(TransactionManager.class);

        BatchProcessor processor =
                new BatchProcessor(targetWriter, transactionManager);

        List<ChangeEvent> events = new ArrayList<>();

        for (int i = 1; i <= 11; i++) {
            events.add(createEvent(i));
        }

        assertThrows(
                IllegalArgumentException.class,
                () -> processor.process(events)
        );

        verifyNoInteractions(targetWriter, transactionManager);
    }

    @Test
    void shouldRollbackWhenWriterFails() throws Exception {

        TargetWriter targetWriter = mock(TargetWriter.class);
        TransactionManager transactionManager = mock(TransactionManager.class);

        doThrow(new RuntimeException("Target write failed"))
                .when(targetWriter)
                .write(any(ChangeEvent.class));

        BatchProcessor processor =
                new BatchProcessor(targetWriter, transactionManager);

        List<ChangeEvent> events = List.of(createEvent(1));

        assertThrows(
                RuntimeException.class,
                () -> processor.process(events)
        );

        verify(transactionManager).begin();
        verify(transactionManager).rollback();
        verify(transactionManager, never()).commit();
    }

    private ChangeEvent createEvent(int customerId) {

        return new ChangeEvent(
                "customers",
                OperationType.INSERT,
                Map.of(
                        "customer_id", customerId,
                        "name", "Test User " + customerId,
                        "email", "test" + customerId + "@gmail.com"
                ),
                "0/123456",
                LocalDateTime.now()
        );
    }
}