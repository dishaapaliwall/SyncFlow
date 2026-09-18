package monitoring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LagMonitorTest {

    @Test
    void shouldUpdateLag() {

        LagMonitor monitor =
                new LagMonitor(300000);

        monitor.updateLag(1000);

        assertEquals(
                1000,
                monitor.getCurrentLagMillis()
        );
    }

    @Test
    void shouldRequireAlertWhenLagExceedsThreshold() {

        LagMonitor monitor =
                new LagMonitor(300000);

        monitor.updateLag(300001);

        assertTrue(monitor.isAlertRequired());
    }

    @Test
    void shouldNotRequireAlertWithinThreshold() {

        LagMonitor monitor =
                new LagMonitor(300000);

        monitor.updateLag(200000);

        assertFalse(monitor.isAlertRequired());
    }

    @Test
    void shouldRejectNegativeLag() {

        LagMonitor monitor =
                new LagMonitor(300000);

        assertThrows(
                IllegalArgumentException.class,
                () -> monitor.updateLag(-1)
        );
    }

    @Test
    void shouldRejectNegativeThreshold() {

        assertThrows(
                IllegalArgumentException.class,
                () -> new LagMonitor(-1)
        );
    }
}