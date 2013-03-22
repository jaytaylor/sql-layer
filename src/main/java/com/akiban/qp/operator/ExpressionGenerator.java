
package com.akiban.qp.operator;

import com.akiban.server.expression.Expression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

public interface ExpressionGenerator {
    Expression getExpression();
    TPreparedExpression getTPreparedExpression();

    enum ErasureMaker {MARK}
}
