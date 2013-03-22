
package com.akiban.server.expression.std;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.explain.Type;
import com.akiban.server.explain.std.ExpressionExplainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Map;

public final class BoundFieldExpression implements Expression {
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    @Override
    public boolean needsRow() {
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(rowBindingPosition, fieldExpression.evaluation());
    }

    @Override
    public AkType valueType() {
        return fieldExpression.valueType();
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("Bound(%d,%s)", rowBindingPosition, fieldExpression.toString());
    }

    // BoundFieldExpression interface

    public BoundFieldExpression(int rowBindingPosition, FieldExpression fieldExpression) {
        this.rowBindingPosition = rowBindingPosition;
        this.fieldExpression = fieldExpression;
    }

    // state
    private final int rowBindingPosition;
    private final FieldExpression fieldExpression;

    @Override
    public String name()
    {
        return "Bound";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        // Extend Field inside, rather than wrapping it.
        CompoundExplainer ex = fieldExpression.getExplainer(context);
        ex.get().remove(Label.NAME); // Want to replace.
        ex.addAttribute(Label.NAME, PrimitiveExplainer.getInstance(name()));
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(rowBindingPosition));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }
    
    public boolean nullIsContaminating()
    {
        return true;
    }

    private static class InnerEvaluation implements ExpressionEvaluation {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(QueryContext context) {
            fieldExpressionEvaluation.of(context.getRow(rowBindingPosition));
        }

        @Override
        public ValueSource eval() {
            return fieldExpressionEvaluation.eval();
        }

        @Override
        public void destroy() {
            fieldExpressionEvaluation.destroy();
        }

        @Override
        public void acquire() {
            fieldExpressionEvaluation.acquire();
        }

        @Override
        public boolean isShared() {
            return fieldExpressionEvaluation.isShared();
        }

        @Override
        public void release() {
            fieldExpressionEvaluation.release();
        }

        private InnerEvaluation(int rowBindingPosition, ExpressionEvaluation fieldExpressionEvaluation) {
            this.rowBindingPosition = rowBindingPosition;
            this.fieldExpressionEvaluation = fieldExpressionEvaluation;
        }

        private final int rowBindingPosition;
        private final ExpressionEvaluation fieldExpressionEvaluation;
    }
}
