
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;

public final class TPreparedBoundField implements TPreparedExpression {
    
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(resultType());
    }

    @Override
    public TInstance resultType() {
        return fieldExpression.resultType();
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(fieldExpression.build(), rowPosition);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        // Extend Field inside, rather than wrapping it.
        CompoundExplainer ex = fieldExpression.getExplainer(context);
        ex.get().remove(Label.NAME); // Want to replace.
        ex.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("Bound"));
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(rowPosition));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }

    @Override
    public String toString() {
        return "Bound(" + rowPosition + ',' + fieldExpression + ')';
    }

    public TPreparedBoundField(RowType rowType, int rowPosition, int fieldPosition) {
        fieldExpression = new TPreparedField(rowType.typeInstanceAt(fieldPosition), fieldPosition);
        this.rowPosition = rowPosition;
    }

    private final TPreparedField fieldExpression;
    private final int rowPosition;

    private static class InnerEvaluation implements TEvaluatableExpression {

        @Override
        public PValueSource resultValue() {
            return fieldEvaluation.resultValue();
        }

        @Override
        public void evaluate() {
            fieldEvaluation.evaluate();
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            fieldEvaluation.with(context.getRow(rowPosition));
        }

        private InnerEvaluation(TEvaluatableExpression fieldEvaluation, int rowPosition) {
            this.fieldEvaluation = fieldEvaluation;
            this.rowPosition = rowPosition;
        }

        private final TEvaluatableExpression fieldEvaluation;
        private final int rowPosition;
    }
}
