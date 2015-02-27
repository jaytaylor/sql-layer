/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator.TestCostModelFactory;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerOperatorCompiler;
import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.sql.server.ServerSessionBase;

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
            initServer(session, store());
            initDone();
        }
    }

    protected class TestSession extends ServerSessionBase {
        public TestSession() {
            super(new ServerServiceRequirements(serviceManager().getLayerInfo(),
                                                dxl(),
                                                serviceManager().getMonitorService(),
                                                serviceManager().getSessionService(),
                                                store(),
                                                configService(),
                                                serviceManager().getServiceByClass(IndexStatisticsService.class),
                                                serviceManager().getServiceByClass(TypesRegistryService.class),
                                                routineLoader(),
                                                txnService(),
                                                serviceManager().getServiceByClass(SecurityService.class),
                                                new TestCostModelFactory(),
                                                serviceManager().getServiceByClass(MetricsService.class),
                                                serviceManager()));
            session = session();
            ais = ais();
            defaultSchemaName = SCHEMA_NAME;
            properties = new Properties();
            properties.put("database", defaultSchemaName);
            properties.put(CONFIG_PARSER_FEATURES, configService().getProperty("fdbsql.sql." + CONFIG_PARSER_FEATURES));
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

}
