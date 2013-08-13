/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryBindings;
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
        if (oneVal != null && oneVal.isNull()) {
            TInstance instance = AkBool.INSTANCE.instance(true);
            return new TPreptimeValue(instance, PValueSources.getNullSource(instance));
        }

        TPreptimeValue rightPrep = right.evaluateConstant(queryContext);
        PValueSource twoVal = rightPrep.value();
        if (twoVal != null && twoVal.isNull()) {
            TInstance instance = AkBool.INSTANCE.instance(true);
            return new TPreptimeValue(instance, PValueSources.getNullSource(instance));
        }

        // Neither side is constant null. If both sides are constant, evaluate
        PValueSource resultSource = null;
        boolean nullable;
        if (oneVal != null && twoVal != null) {
            final boolean result = doEval(leftPrep.instance(), oneVal, rightPrep.instance(), twoVal);
            resultSource = new PValue(AkBool.INSTANCE.instance(false), result);
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
                value = new PValue(AkBool.INSTANCE.instance(true));
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

        @Override
        public void with(QueryBindings bindings) {
            left.with(bindings);
            right.with(bindings);
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
