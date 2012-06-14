/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
