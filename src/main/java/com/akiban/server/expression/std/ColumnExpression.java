
package com.akiban.server.expression.std;

import com.akiban.ais.model.Column;
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

/**
 * This is similar to a FieldExpression, except that fields are specified by an AIS Column, rather than
 * by RowType + int.
 */
public final class ColumnExpression implements Expression {

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return false;
    }

    @Override
    public boolean needsRow() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(position);
    }

    @Override
    public AkType valueType() {
        return column.getType().akType();
    }

    // Object interface


    @Override
    public String toString() {
        return String.format("Field(%d)", position);
    }

    public ColumnExpression(Column column, int position) {
        this.column = column;
        this.position = position;
    }

    private final Column column;
    private final int position;

    @Override
    public String name()
    {
        return "FIELD";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.FUNCTION, name(), context);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(position));
        return ex;
    }
    
    public boolean nullIsContaminating()
    {
        return true;
    }

    // nested classes

    private static class InnerEvaluation extends ExpressionEvaluation.Base {

        // ExpressionEvaluation interface

        @Override
        public void of(Row row) {
            this.row = row;
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public ValueSource eval() {
            if (row == null)
                throw new IllegalStateException("haven't seen a row to target");
            return row.eval(position);
        }

        // Shareable interface

        @Override
        public void acquire() {
            row.acquire();
        }

        @Override
        public boolean isShared() {
            return row.isShared();
        }

        @Override
        public void release() {
            row.release();
        }

        // private methods

        private InnerEvaluation(int position) {
            this.position = position;
        }

        private final int position;
        private Row row;
    }
}
