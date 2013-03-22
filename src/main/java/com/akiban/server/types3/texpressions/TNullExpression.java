
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.TExpressionExplainer;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

public final class TNullExpression implements TPreparedExpression {

    public TNullExpression (TInstance tInstance) {
        this.tInstance = tInstance.withNullable(true);
    }

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        TEvaluatableExpression eval = build();
        return new TPreptimeValue(tInstance, eval.resultValue());
    }

    @Override
    public TInstance resultType() {
        return tInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(tInstance);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.LITERAL, "Literal", context);
        ex.addAttribute(Label.OPERAND, PrimitiveExplainer.getInstance("NULL"));
        return ex;
    }

    @Override
    public String toString() {
        return "Literal(NULL)";
    }

    private final TInstance tInstance;

    private static class InnerEvaluation implements TEvaluatableExpression {
        @Override
        public PValueSource resultValue() {
            return valueSource;
        }

        @Override
        public void evaluate() {
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
        }

        private InnerEvaluation(TInstance underlying) {
            this.valueSource = PValueSources.getNullSource(underlying);
        }

        private final PValueSource valueSource;
    }
}
