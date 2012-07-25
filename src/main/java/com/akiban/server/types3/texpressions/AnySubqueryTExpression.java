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

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueTarget;

public final class AnySubqueryTExpression extends SubqueryTExpression {

    @Override
    public TInstance resultType() {
        return AkBool.INSTANCE.instance();
    }

    @Override
    public TEvaluatableExpression build(QueryContext queryContext) {
        TEvaluatableExpression child = expression.build(null);
        return new InnerEvaluatable(subquery(), child, outerRowType(), innerRowType(), bindingPosition());
    }

    public AnySubqueryTExpression(Operator subquery, TPreparedExpression expression,
                                  RowType outerRowType, RowType innerRowType, int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        this.expression = expression;
    }

    private final TPreparedExpression expression;

    private static class InnerEvaluatable extends SubqueryTEvaluateble {

        @Override
        protected void doEval(PValueTarget out) {
            evaluation.with(queryContext());
            while (true) {
                Row row = next();
                if (row == null) break;
                evaluation.with(row);
                evaluation.evaluate();
                if (evaluation.resultValue().isNull()) {
                    out.putNull();
                }
                else if (evaluation.resultValue().getBoolean()) {
                    out.putBool(true);
                    break;
                }
            }
        }

        private InnerEvaluatable(Operator subquery, TEvaluatableExpression evaluation, RowType outerRowType,
                                       RowType innerRowType, int bindingPosition)
        {
            super(subquery, outerRowType, innerRowType, bindingPosition, PUnderlying.BOOL);
            this.evaluation = evaluation;
        }

        private final TEvaluatableExpression evaluation;
    }
}
