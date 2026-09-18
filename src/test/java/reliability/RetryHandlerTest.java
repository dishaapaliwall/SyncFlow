package reliability;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RetryHandlerTest {

    @Test
    void shouldAllowRetry() {

        RetryHandler handler =
                new RetryHandler(3);

        assertTrue(handler.shouldRetry(0));
        assertTrue(handler.shouldRetry(1));
        assertTrue(handler.shouldRetry(2));
    }

    @Test
    void shouldStopAfterMaximumRetries() {

        RetryHandler handler =
                new RetryHandler(3);

        assertFalse(handler.shouldRetry(3));
        assertFalse(handler.shouldRetry(4));
    }

    @Test
    void shouldRejectNegativeMaxRetries() {

        assertThrows(
                IllegalArgumentException.class,
                () -> new RetryHandler(-1)
        );
    }

    @Test
    void shouldReturnConfiguredMaxRetries() {

        RetryHandler handler =
                new RetryHandler(5);

        assertEquals(5, handler.getMaxRetries());
    }
}
