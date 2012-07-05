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

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.IntValueSource;
import com.akiban.server.types.util.ValueHolder;

import com.akiban.server.types.util.ValueSources;
import java.util.List;

public final class RankExpression extends CompareExpression {

    // AbstractTwoArgExpression interfac

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public boolean nullIsContaminating() {
        return false;
    }

    public RankExpression(Expression lhs, Expression rhs) {
        super(lhs, rhs);
    }

    // consts

    private static final String RANKING_DESCRIPTION = ">=<";

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation {
        @Override
        public ValueSource eval() {
            int comparison;
            ValueSource left = scratch.copyFrom(left());
            ValueSource right = right();
            boolean leftNull = left.isNull();
            boolean rightNull = right.isNull();
            if (!leftNull && !rightNull) {
                // overflow could happen and lead to incorrect result if d(left,right) > Integer.MAX_VALUE
                comparison = (int)ValueSources.compare(left, right);
            } else if (!leftNull) {
                comparison = 1;
            } else if (!rightNull) {
                comparison = -1;
            } else {
                comparison = 0;
            }
            return IntValueSource.of(comparison);
        }

        private InnerEvaluation(List<? extends ExpressionEvaluation> children) {
            super(children);
            this.scratch = new ValueHolder();
        }

        private final ValueHolder scratch;
    }
}
