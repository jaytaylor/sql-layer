
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.explain.*;
import com.akiban.server.explain.std.TExpressionExplainer;
import com.akiban.server.types3.ErrorHandlingMode;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

public final class TPreparedParameter implements TPreparedExpression {

    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        return new TPreptimeValue(tInstance);
    }

    @Override
    public TInstance resultType() {
        return tInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new InnerEvaluation(position, tInstance);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new TExpressionExplainer(Type.VARIABLE, "Variable", context);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(position));
        return ex;
    }

    @Override
    public String toString() {
        return "Variable(pos=" + position + ')';
    }

    public TPreparedParameter(int position, TInstance tInstance) {
        this.position = position;
        this.tInstance = tInstance;
    }

    private final int position;
    private final TInstance tInstance;

    private static class InnerEvaluation implements TEvaluatableExpression {

        @Override
        public PValueSource resultValue() {
            return pValue;
        }

        @Override
        public void evaluate() {
            if (!pValue.hasAnyValue()) { // only need to compute this once
                TClass tClass = tInstance.typeClass();
                PValueSource inSource = context.getPValue(position);
                // TODO need a better execution context thinger
                TExecutionContext executionContext = new TExecutionContext(
                        null,
                        null,
                        tInstance,
                        context,
                        ErrorHandlingMode.WARN,
                        ErrorHandlingMode.WARN,
                        ErrorHandlingMode.WARN
                );
                tClass.fromObject(executionContext, inSource, pValue);
            }
        }

        @Override
        public void with(Row row) {
        }

        @Override
        public void with(QueryContext context) {
            this.context = context;
        }

        private InnerEvaluation(int position, TInstance tInstance) {
            this.position = position;
            this.tInstance = tInstance;
            this.pValue = new PValue(tInstance);
        }

        private final int position;
        private final PValue pValue;
        private QueryContext context;
        private final TInstance tInstance;
    }
}
