
package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public final class ResultSetSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), outerRowType(),
                                   bindingPosition());
    }

    @Override
    public AkType valueType() {
        return AkType.RESULT_SET;
    }

    @Override
    public String toString() {
        return "RESULT_SET(" + subquery() + ")";
    }

    @Override
    public String name () {
        return "RESULT_SET";
    }
    
    public ResultSetSubqueryExpression(Operator subquery,
                                       RowType outerRowType, RowType innerRowType, 
                                       int bindingPosition) {
        super(subquery, outerRowType, innerRowType, bindingPosition);
    }

    // TODO: Could refactor SubqueryExpressionEvaluation into a common piece.
    private static final class InnerEvaluation extends ExpressionEvaluation.Base {
        @Override
        public void of(QueryContext context) {
            this.context = context;
        }

        @Override
        public void of(Row row) {
            if (row.rowType() != outerRowType) {
                throw new IllegalArgumentException("wrong row type: " + outerRowType +
                                                   " != " + row.rowType());
            }
            outerRow = row;
        }

        @Override
        public ValueSource eval() {
            context.setRow(bindingPosition, outerRow);
            Cursor cursor = API.cursor(subquery, context);
            cursor.open();
            return new ValueHolder(AkType.RESULT_SET, cursor);
        }

        // Shareable interface

        @Override
        public void acquire() {
            outerRow.acquire();
        }

        @Override
        public boolean isShared() {
            return outerRow.isShared();
        }

        @Override
        public void release() {
            outerRow.release();
        }

        protected InnerEvaluation(Operator subquery,
                                  RowType outerRowType,
                                  int bindingPosition) {
            this.subquery = subquery;
            this.outerRowType = outerRowType;
            this.bindingPosition = bindingPosition;
        }

        private final Operator subquery;
        private final RowType outerRowType;
        private final int bindingPosition;
        private QueryContext context;
        private Row outerRow;
    }

}
