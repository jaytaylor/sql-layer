/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.OperatorCompiler;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.session.Session;

public class ExplainPlanContext extends PlanContext
{
    private PlanExplainContext explainContext = new PlanExplainContext();
    private ServiceManager serviceManager;
    private Session session;

    public ExplainPlanContext(OperatorCompiler rulesContext, 
                              ServiceManager serviceManager, Session session) {
        super(rulesContext);
        this.serviceManager = serviceManager;
        this.session = session;
    }

    public PlanExplainContext getExplainContext() {
        return explainContext;
    }

    @Override
    public QueryContext getQueryContext() {
        return new SimpleQueryContext(null) {
                @Override
                public Session getSession() {
                    return session;
                }
                @Override
                public ServiceManager getServiceManager() {
                    return serviceManager;
                }
            };
    }

}
