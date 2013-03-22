
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.TExpressionExplainer;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;

public final class TPreparedField implements TPreparedExpression {
    
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return null;
    }

    @Override
    public TInstance resultType() {
        return typeInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new Evaluation(typeInstance, fieldIndex);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.FIELD, "Field", context);
        ex.addAttribute(Label.POSITION, PrimitiveExplainer.getInstance(fieldIndex));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    @Override
    public String toString() {
        return "Field(" + fieldIndex + ')';
    }

    public TPreparedField(TInstance typeInstance, int fieldIndex) {
        this.typeInstance = typeInstance;
        this.fieldIndex = fieldIndex;
    }

    private final TInstance typeInstance;
    private final int fieldIndex;
    
    private static class Evaluation extends ContextualEvaluation<Row> {
        @Override
        protected void evaluate(Row context, PValueTarget target) {
            PValueSource rowSource = context.pvalue(fieldIndex);
            PValueTargets.copyFrom(rowSource, target);
        }

        @Override
        public void with(Row row) {
            setContext(row);
        }

        private Evaluation(TInstance underlyingType, int fieldIndex) {
            super(underlyingType);
            this.fieldIndex = fieldIndex;
        }

        private int fieldIndex;
    }
}
