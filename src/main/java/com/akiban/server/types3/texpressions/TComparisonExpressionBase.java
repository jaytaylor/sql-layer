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

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.TExpressionExplainer;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

public abstract class TComparisonExpressionBase implements TPreparedExpression {

    protected abstract int compare(TInstance leftInstance, PValueSource left,
                                   TInstance rightInstance, PValueSource right);

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        // First check both sides. If either is a constant null, the result is null
        TPreptimeValue leftPrep = left.evaluateConstant(queryContext);
        PValueSource oneVal = leftPrep.value();
        if (oneVal != null && oneVal.isNull())
            return new TPreptimeValue(AkBool.INSTANCE.instance(true), PValueSources.getNullSource(AkBool.INSTANCE));

        TPreptimeValue rightPrep = right.evaluateConstant(queryContext);
        PValueSource twoVal = rightPrep.value();
        if (twoVal != null && twoVal.isNull())
            return new TPreptimeValue(AkBool.INSTANCE.instance(true), PValueSources.getNullSource(AkBool.INSTANCE));

        // Neither side is constant null. If both sides are constant, evaluate
        PValueSource resultSource = null;
        boolean nullable;
        if (oneVal != null && twoVal != null) {
            final boolean result = doEval(leftPrep.instance(), oneVal, rightPrep.instance(), twoVal);
            resultSource = new PValue(AkBool.INSTANCE, result);
            nullable = resultSource.isNull();
        }
        else {
            nullable = left.resultType().nullability() || right.resultType().nullability();
        }
        return new TPreptimeValue(MNumeric.INT.instance(nullable), resultSource);
    }

    @Override
    public TInstance resultType() {
        return AkBool.INSTANCE.instance(left.resultType().nullability() || right.resultType().nullability());
    }

    @Override
    public TEvaluatableExpression build() {
        TInstance leftInstance = left.resultType();
        TEvaluatableExpression leftEval = left.build();
        TInstance rightInstance = right.resultType();
        TEvaluatableExpression rightEval = right.build();
        return new CompareEvaluation(leftInstance, leftEval, rightInstance, rightEval);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer ex = new TExpressionExplainer(Type.BINARY_OPERATOR, comparison.toString(), context, left, right);
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance(comparison.toString()));
        return ex;
    }

    @Override
    public String toString() {
        return left + " " + comparison + ' ' + right;
    }

    public TComparisonExpressionBase(TPreparedExpression left, Comparison comparison, TPreparedExpression right) {
        this.left = left;
        this.comparison = comparison;
        this.right = right;
    }

    private boolean doEval(TInstance leftInstance, PValueSource left, TInstance rightInstance, PValueSource right) {
        int cmpI = compare(leftInstance, left, rightInstance, right);
        final Comparison actualComparison;

        if (cmpI == 0)
            actualComparison = Comparison.EQ;
        else if (cmpI < 0)
            actualComparison = Comparison.LT;
        else
            actualComparison = Comparison.GT;

        final boolean result;
        switch (actualComparison) {
        case EQ:
            result = (comparison == Comparison.EQ) || (comparison == Comparison.LE) || (comparison == Comparison.GE);
            break;
        case GT:
            result = (comparison == Comparison.GT || comparison == Comparison.GE || comparison == Comparison.NE);
            break;
        case LT:
            result = (comparison == Comparison.LT || comparison == Comparison.LE || comparison == Comparison.NE);
            break;
        default:
            throw new AssertionError(actualComparison);
        }
        return result;
    }

    private final Comparison comparison;
    private final TPreparedExpression left;
    private final TPreparedExpression right;

    private class CompareEvaluation implements TEvaluatableExpression {

        @Override
        public PValueSource resultValue() {
            if (value == null)
                throw new IllegalStateException("not evaluated");
            return value;
        }

        @Override
        public void evaluate() {
            if (value == null)
                value = new PValue(AkBool.INSTANCE);
            left.evaluate();
            PValueSource leftSource = left.resultValue();
            if (leftSource.isNull()) {
                value.putNull();
                return;
            }
            right.evaluate();
            PValueSource rightSource = right.resultValue();
            if (rightSource.isNull()) {
                value.putNull();
                return;
            }

            boolean result = doEval(leftInstance, leftSource, rightInstance, rightSource);
            value.putBool(result);
        }

        @Override
        public void with(Row row) {
            left.with(row);
            right.with(row);
        }

        @Override
        public void with(QueryContext context) {
            left.with(context);
            right.with(context);
        }

        private CompareEvaluation(TInstance leftInstance, TEvaluatableExpression left,
                                  TInstance rightInstance, TEvaluatableExpression right)
        {
            this.leftInstance = leftInstance;
            this.rightInstance = rightInstance;
            this.left = left;
            this.right = right;
        }

        private final TInstance leftInstance;
        private final TInstance rightInstance;
        private final TEvaluatableExpression left;
        private final TEvaluatableExpression right;
        private PValue value;
    }
}
