
package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;

import java.util.List;

public abstract class AbstractTwoArgExpressionEvaluation extends AbstractCompositeExpressionEvaluation {

    public ExpressionEvaluation leftEvaluation() {
        return children().get(0);
    }

    protected final ValueSource left() {
        return leftEvaluation().eval();
    }

    public ExpressionEvaluation rightEvaluation() {
        return children().get(1);
    }

    protected final ValueSource right() {
        return rightEvaluation().eval();
    }

    protected AbstractTwoArgExpressionEvaluation(List<? extends ExpressionEvaluation> children) {
        super(children);
        if (children().size() != 2) {
            throw new IllegalArgumentException("required 2 children, but saw" + children().size());
        }
    }
}
