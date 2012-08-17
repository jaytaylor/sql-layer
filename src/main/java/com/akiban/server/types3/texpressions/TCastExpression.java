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
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.SparseArray;

import java.util.Collections;

public final class TCastExpression implements TPreparedExpression {
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        TPreptimeValue inputValue = input.evaluateConstant(queryContext);
        PValue value;
        if (inputValue == null || inputValue.value() == null) {
            value = null;
        }
        else {
            value = new PValue(cast.targetClass().underlyingType());

            TExecutionContext context = new TExecutionContext(
                    new SparseArray<Object>(),
                    Collections.singletonList(input.resultType()),
                    targetInstance,
                    queryContext,
                    null,
                    null,
                    null
            );
            cast.evaluate(context, inputValue.value(), value);
        }
        return new TPreptimeValue(targetInstance, value);
    }

    @Override
    public TInstance resultType() {
        return targetInstance;
    }

    @Override
    public TEvaluatableExpression build() {
        return new CastEvaluation(input.build(), cast, sourceInstance, targetInstance, preptimeContext);
    }

    @Override
    public String toString() {
        return input.toString(); // for backwards compatibility in OperatorCompilerTest, don't actually print the cast
    }

    public TCastExpression(TPreparedExpression input, TCast cast, TInstance targetInstance, QueryContext queryContext) {
        this.input = input;
        this.cast = cast;
        this.targetInstance = targetInstance;
        this.sourceInstance = input.resultType();
        this.preptimeContext = queryContext;
        assert sourceInstance.typeClass() == cast.sourceClass()
                : sourceInstance + " not an acceptable source for cast " + cast;
    }


    private final TInstance sourceInstance;
    private final TInstance targetInstance;
    private final TPreparedExpression input;
    private final TCast cast;
    private final QueryContext preptimeContext;

    private static class CastEvaluation implements TEvaluatableExpression {
        @Override
        public PValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            inputEval.evaluate();
            PValueSource inputVal = inputEval.resultValue();
            if (inputVal.isNull())
                value.putNull();
            else
                cast.evaluate(executionContext, inputVal, value);
        }

        @Override
        public void with(Row row) {
            inputEval.with(row);
        }

        @Override
        public void with(QueryContext context) {
            inputEval.with(context);
            this.executionContext.setQueryContext(context);
        }

        private CastEvaluation(TEvaluatableExpression inputEval, TCast cast,
                               TInstance sourceInstance, TInstance targetInstance, QueryContext preptimeContext)
        {
            this.inputEval = inputEval;
            this.cast = cast;
            this.value = new PValue(cast.targetClass().underlyingType());
            this.executionContext = new TExecutionContext(
                    Collections.singletonList(sourceInstance),
                    targetInstance,
                    preptimeContext);
        }

        private final TExecutionContext executionContext;
        private final TEvaluatableExpression inputEval;
        private final TCast cast;
        private final PValue value;
    }
}
