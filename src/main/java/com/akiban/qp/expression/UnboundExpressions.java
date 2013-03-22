
package com.akiban.qp.expression;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.CompoundExplainer;

public interface UnboundExpressions {
    BoundExpressions get(QueryContext context);
    CompoundExplainer getExplainer(ExplainContext context);
}
