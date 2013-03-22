
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
