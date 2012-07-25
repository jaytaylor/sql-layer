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
import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

public final class TComparisonExpression implements TPreparedExpression {
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        // First check both sides. If either is a constant null, the result is null
        TPreptimeValue leftPrep = left.evaluateConstant(queryContext);
        PValueSource oneVal = leftPrep.value();
        if (oneVal != null && oneVal.isNull())
            return new TPreptimeValue(AkBool.INSTANCE.instance(), PValueSources.getNullSource(PUnderlying.BOOL));

        TPreptimeValue rightPrep = right.evaluateConstant(queryContext);
        PValueSource twoVal = rightPrep.value();
        if (twoVal != null && twoVal.isNull())
            return new TPreptimeValue(AkBool.INSTANCE.instance(), PValueSources.getNullSource(PUnderlying.BOOL));

        // Neither side is constant null. If both sides are constant, evaluate
        PValueSource resultSource = null;
        if (oneVal != null && twoVal != null) {
            PValue result = new PValue(PUnderlying.BOOL);
            doEvaluate(leftPrep.instance(), oneVal, comparison, rightPrep.instance(), twoVal, collator, result);
            resultSource = result;
        }
        return new TPreptimeValue(MNumeric.INT.instance(), resultSource);
    }

    @Override
    public TInstance resultType() {
        return AkBool.INSTANCE.instance();
    }

    @Override
    public TEvaluatableExpression build(QueryContext queryContext) {
        TInstance leftInstance = left.resultType();
        TEvaluatableExpression leftEval = left.build(null);
        TInstance rightInstance = right.resultType();
        TEvaluatableExpression rightEval = right.build(null);
        return new CompareEvaluation(leftInstance, leftEval, comparison, rightInstance, rightEval, collator);
    }

    @Override
    public String toString() {
        return left + " " + comparison + ' ' + right;
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right) {
        this(left, comparison, right, null);
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right,
                                 AkCollator collator) {
        TClass oneClass = left.resultType().typeClass();
        TClass twoClass = right.resultType().typeClass();
        if (!oneClass.equals(twoClass))
            throw new IllegalArgumentException("can't compare expressions of different types: " + left + " != " + right);
        if (collator != null && oneClass.underlyingType() != PUnderlying.STRING) {
            throw new IllegalArgumentException("collator provided, but " + oneClass + " is not a string type");
        }
        this.left = left;
        this.comparison = comparison;
        this.right = right;
        this.collator = collator;
    }

    private final Comparison comparison;
    private final TPreparedExpression left;
    private final TPreparedExpression right;
    private final AkCollator collator;

    private static void doEvaluate(TInstance leftInstance, PValueSource leftSource,
                                   Comparison comparison,
                                   TInstance rightInstance, PValueSource rightSource,
                                   AkCollator collator,
                                   PValue value)
    {
        TClass tClass = leftInstance.typeClass();
        assert rightInstance.typeClass() == tClass
                : "type class mismatch: " + leftInstance + " != " + rightInstance;
        final int cmpI;
        if (collator != null) {
            assert tClass.underlyingType() == PUnderlying.STRING : tClass + ": " + tClass.underlyingType();
            String leftString = leftSource.getString();
            String rightString = rightSource.getString();
            cmpI = collator.compare(leftString, rightString);
        }
        else {
            cmpI = TClass.compare(leftInstance, leftSource, rightInstance, rightSource);
        }
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
            result = (comparison == Comparison.EQ);
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

        value.putBool(result);
    }

    private static class CompareEvaluation implements TEvaluatableExpression {

        @Override
        public PValueSource resultValue() {
            if (value == null)
                throw new IllegalStateException("not evaluated");
            return value;
        }

        @Override
        public void evaluate() {
            if (value == null)
                value = new PValue(PUnderlying.BOOL);
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

            doEvaluate(leftInstance, leftSource, comparison, rightInstance, rightSource, collator,  value);
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
                                  Comparison comparison,
                                  TInstance rightInstance, TEvaluatableExpression right,
                                  AkCollator collator)
        {
            this.leftInstance = leftInstance;
            this.left = left;
            this.comparison = comparison;
            this.right = right;
            this.rightInstance = rightInstance;
            this.collator = collator;
        }

        private final Comparison comparison;
        private final TInstance leftInstance;
        private final TInstance rightInstance;
        private final TEvaluatableExpression left;
        private final TEvaluatableExpression right;
        private final AkCollator collator;
        private PValue value;
    }
}
