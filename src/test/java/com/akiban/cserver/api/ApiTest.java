package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.session.UnitTestServiceManagerFactory;
import com.akiban.cserver.store.Store;
import org.junit.Test;

import static junit.framework.Assert.*;

public final class ApiTest {

    private static class ApiPair {
        final DMLFunctionsImpl dml;
        final DDLFunctionsImpl ddl;

        private ApiPair() {
            final Session session = new SessionImpl();
            final Store store;
            try {
                store = UnitTestServiceManagerFactory.getStoreForUnitTests();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LoggingService loggingService = new LoggingServiceImpl();
            dml = new DMLFunctionsImpl(store, loggingService);
            ddl = new DDLFunctionsImpl(store);

        }
    }

    @Test
    public void testAutoIncrement() throws InvalidOperationException {
        ApiPair apiPair = new ApiPair();
        final TableId tableId = TableId.of("sc1", "t1");
        apiPair.ddl.createTable("sc1", "CREATE TABLE t1(c1 TINYINT   AUTO_INCREMENT NULL KEY ) AUTO_INCREMENT=10");
        TableStatistics tableStats = apiPair.dml.getTableStatistics(tableId, false);
        assertEquals("autoinc value", 10L, tableStats.getAutoIncrementValue());
    }
}
