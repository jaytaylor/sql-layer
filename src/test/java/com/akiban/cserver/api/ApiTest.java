package com.akiban.cserver.api;

import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;

import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.session.UnitTestServiceManagerFactory;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.Store;

public final class ApiTest extends TestCase {

    private PersistitStore store;

    protected final static Session session = new SessionImpl();

    @Override
    public void setUp() throws Exception {
        store = UnitTestServiceManagerFactory.getStoreForUnitTests();
    }

    @Override
    public void tearDown() throws Exception {
        store.stop();
        store = null;
    }

    private class ApiPair {
        final DMLFunctionsImpl dml;
        final DDLFunctionsImpl ddl;
        
        private ApiPair() {
            final Session session = new SessionImpl();
            LoggingService loggingService = new LoggingServiceImpl();
            dml = new DMLFunctionsImpl(store, loggingService);
            ddl = new DDLFunctionsImpl(store);

        }
    }

    @Test
    public void testAutoIncrement() throws InvalidOperationException {
        ApiPair apiPair = new ApiPair();
        final TableId tableId = TableId.of("sc1", "t1");
        final Session session = new SessionImpl();
        apiPair.ddl.createTable(session, "sc1", "CREATE TABLE t1(c1 TINYINT   AUTO_INCREMENT NULL KEY ) AUTO_INCREMENT=10");
        TableStatistics tableStats = apiPair.dml.getTableStatistics(session, tableId, false);
        assertEquals("autoinc value", 10L, tableStats.getAutoIncrementValue());
    }
}
