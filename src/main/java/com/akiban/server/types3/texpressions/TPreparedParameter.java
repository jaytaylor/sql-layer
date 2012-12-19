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
            this.pValue = new PValue(tInstance.typeClass());
        }

        private final int position;
        private final PValue pValue;
        private QueryContext context;
        private final TInstance tInstance;
    }
}
