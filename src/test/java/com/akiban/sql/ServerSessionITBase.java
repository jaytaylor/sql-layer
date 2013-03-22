
package com.akiban.sql;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.test.it.ITBase;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerOperatorCompiler;
import com.akiban.sql.server.ServerServiceRequirements;
import com.akiban.sql.server.ServerSessionBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ServerSessionITBase extends ITBase {
    public static final String SCHEMA_NAME = "test";

    protected List<String> warnings = null;

    protected List<String> getWarnings() {
        return warnings;
    }

    protected class TestQueryContext extends ServerQueryContext<TestSession> {
        public TestQueryContext(TestSession session) {
            super(session);
        }
    }

    protected class TestOperatorCompiler extends ServerOperatorCompiler {
        public TestOperatorCompiler(TestSession session) {
            initServer(session);
            initDone();
        }
    }

    protected class TestSession extends ServerSessionBase {
        public TestSession() {
            super(new ServerServiceRequirements(serviceManager().getAkSserver(),
                                                dxl(),
                                                serviceManager().getMonitorService(),
                                                serviceManager().getSessionService(),
                                                store(),
                                                treeService(),
                                                serviceManager().getServiceByClass(com.akiban.server.service.functions.FunctionsRegistry.class),
                                                configService(),
                                                serviceManager().getServiceByClass(com.akiban.server.store.statistics.IndexStatisticsService.class),
                                                serviceManager().getServiceByClass(com.akiban.server.t3expressions.T3RegistryService.class),
                                                routineLoader(),
                                                txnService(),
                                                new DummySecurityService(),
                                                serviceManager()));
            session = session();
            ais = ais();
            defaultSchemaName = SCHEMA_NAME;
            properties = new Properties();
            properties.put("database", defaultSchemaName);
            initParser();        
            TestOperatorCompiler compiler = new TestOperatorCompiler(this);
            initAdapters(compiler);
        }

        @Override
        protected void sessionChanged() {
        }

        @Override
        public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) {
            if (warnings == null)
                warnings = new ArrayList<>();
            warnings.add(message);
        }
    }

    protected static class DummySecurityService implements com.akiban.server.service.security.SecurityService {
        @Override
        public com.akiban.server.service.security.User authenticate(com.akiban.server.service.session.Session session, String name, String password) {
            return null;
        }

        @Override
        public com.akiban.server.service.security.User authenticate(com.akiban.server.service.session.Session session, String name, String password, byte[] salt) {
            return null;
        }

        @Override
        public boolean isAccessible(com.akiban.server.service.session.Session session, String schema) {
            return true;
        }

        @Override
        public boolean isAccessible(javax.servlet.http.HttpServletRequest request, String schema) {
            return true;
        }

        @Override
        public void addRole(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRole(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.akiban.server.service.security.User getUser(String name) {
            return null;
        }

        @Override
        public com.akiban.server.service.security.User addUser(String name, String password, java.util.Collection<String> roles) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteUser(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void changeUserPassword(String name, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearAll(com.akiban.server.service.session.Session session) {
        }
    }

}
