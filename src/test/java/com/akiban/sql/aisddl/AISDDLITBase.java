/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.aisddl;

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

public class AISDDLITBase extends ITBase {
    public static final String SCHEMA_NAME = "test";

    protected List<String> warnings = null;

    protected List<String> getWarnings() {
        return warnings;
    }

    protected void executeDDL(String sql) throws Exception {
        // Most of the state in this depends on the current AIS, which changes
        // as a result of this, so it's simplest to just make a new session
        // every time. Only views need all of the binder state, but
        // it's just as easy to make the parser this way.
        TestSession session = new TestSession();
        StatementNode stmt = session.getParser().parseStatement(sql);
        assert (stmt instanceof DDLStatementNode) : stmt;
        AISDDL.execute((DDLStatementNode)stmt, new TestQueryContext(session));
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
                                                serviceManager().getServiceByClass(com.akiban.server.service.routines.RoutineLoader.class),
                                                txnService()));
            session = session();
            ais = ais();
            defaultSchemaName = SCHEMA_NAME;
            properties = new Properties();
            properties.put("database", defaultSchemaName);
            initParser();        
            TestOperatorCompiler compiler = new TestOperatorCompiler(this);
        }

        @Override
        protected void sessionChanged() {
        }

        @Override
        public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) {
            if (warnings == null)
                warnings = new ArrayList<String>();
            warnings.add(message);
        }
    }

}
