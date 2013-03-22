
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;

public abstract class AbstractBinaryExpression extends AbstractCompositeExpression {

    protected final Expression left() {
        return children().get(0);
    }

    protected final Expression right() {
        return children().get(1);
    }

    protected AbstractBinaryExpression(AkType type, Expression first, Expression second) {
        super(type, first, second);
        if (children().size() != 2) {
            throw new WrongExpressionArityException(2, children().size());
        }
    }
}
