
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;

public interface ResolvableExpression<T> extends ExpressionNode {
    String getFunction();
    void setResolved(T resolved);
    T getResolved();
    TPreptimeContext getPreptimeContext();
    void setPreptimeContext(TPreptimeContext context);
}
