package com.akiban.cserver.api;

import org.junit.Test;

import com.akiban.cserver.CServerTestCase;
import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;

public final class ApiTest extends CServerTestCase {

    private class ApiPair {
        final DMLFunctionsImpl dml;
        final DDLFunctionsImpl ddl;
        
        private ApiPair() {
            LoggingService loggingService = new LoggingServiceImpl();
            dml = new DMLFunctionsImpl(loggingService);
            ddl = new DDLFunctionsImpl();
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
