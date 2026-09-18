package sync;

import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.mockito.Mockito.*;

class TransactionManagerTest {

    @Test
    void shouldBeginTransaction() throws Exception {

        Connection connection = mock(Connection.class);

        TransactionManager manager = new TransactionManager(connection);

        manager.begin();

        verify(connection).setAutoCommit(false);
    }

    @Test
    void shouldCommitTransaction() throws Exception {

        Connection connection = mock(Connection.class);

        TransactionManager manager = new TransactionManager(connection);

        manager.commit();

        verify(connection).commit();
        verify(connection).setAutoCommit(true);
    }

    @Test
    void shouldRollbackTransaction() throws Exception {

        Connection connection = mock(Connection.class);

        TransactionManager manager = new TransactionManager(connection);

        manager.rollback();

        verify(connection).rollback();
        verify(connection).setAutoCommit(true);
    }
}