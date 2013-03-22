
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
import java.util.List;
import java.util.Map;

public final class VariableExpression implements Expression {

    // Expression interface

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
        return new InnerEvaluation(type, position);
    }

    @Override
    public AkType valueType() {
        return type;
    }

    public VariableExpression(AkType type, int position) {
        this.type = type;
        this.position = position;
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("Variable(pos=%d)", position);
    }

    // object state

    private final AkType type;
    private final int position;

    @Override
    public String name()
    {
        return "Variable";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.VARIABLE, name(), context);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(position));
        return ex;
    }

    public boolean nullIsContaminating()
    {
        return true;
    }

    private static class InnerEvaluation extends ExpressionEvaluation.Base {

        @Override
        public void of(Row row) {
        }

        @Override
        public void of(QueryContext context) {
            this.context = context;
        }

        @Override
        public ValueSource eval() {
            return context.getValue(position);
        }

        @Override
        public void acquire() {
            ++ownedBy;
        }

        @Override
        public boolean isShared() {
            return ownedBy > 1;
        }

        @Override
        public void release() {
            assert ownedBy > 0 : ownedBy;
            --ownedBy;
        }

        private InnerEvaluation(AkType type, int position) {
            this.type = type;
            this.position = position;
        }

        private final AkType type;
        private final int position;
        private QueryContext context;
        private int ownedBy = 0;
    }
}
