
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.explain.Explainable;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;

public interface TPreparedExpression extends Explainable {
    TPreptimeValue evaluateConstant(QueryContext queryContext);
    TInstance resultType();
    TEvaluatableExpression build();
}
