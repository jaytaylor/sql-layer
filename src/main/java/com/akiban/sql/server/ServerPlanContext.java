
package com.akiban.sql.server;

import com.akiban.sql.optimizer.rule.PlanContext;

import com.akiban.qp.operator.QueryContext;

public class ServerPlanContext extends PlanContext
{
    private ServerQueryContext<?> queryContext;

    public ServerPlanContext(ServerOperatorCompiler rulesContext, ServerQueryContext<?> queryContext) {
        super(rulesContext);
        this.queryContext = queryContext;
    }

    @Override
    public QueryContext getQueryContext() {
        return queryContext;
    }

}
