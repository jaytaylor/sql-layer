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
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

import java.util.Collections;

public final class TCastExpression implements TPreparedExpression {
    @Override
    public TPreptimeValue evaluateConstant(QueryContext queryContext) {
        PValue value;
        if (inputValue.value() == null) {
            value = null;
        }
        else {
            value = new PValue(cast.targetClass().underlyingType());
            cast.evaluate(null, inputValue.value(), value);
        }
        return new TPreptimeValue(targetInstance, value);
    }

    @Override
    public TInstance resultType() {
        return targetInstance;
    }

    @Override
    public TEvaluatableExpression build(QueryContext queryContext) {
        TExecutionContext context = preptimeContext.createExecutionContext();
        return new CastEvaluation(input.build(null), context, cast);
    }

    @Override
    public String toString() {
        return input.toString(); // for backwards compatibility in OperatorCompilerTest, don't actually print the cast
    }

    public TCastExpression(TPreparedExpression input, TCast cast, TInstance targetInstance) {
        this.input = input;
        this.cast = cast;
        inputValue = input.evaluateConstant(null);
        this.targetInstance = targetInstance;
        this.preptimeContext = new TPreptimeContext(Collections.singletonList(input.resultType()), targetInstance);
    }


    private final TPreptimeValue inputValue;
    private final TInstance targetInstance;
    private final TPreparedExpression input;
    private final TCast cast;
    private final TPreptimeContext preptimeContext;

    private static class CastEvaluation implements TEvaluatableExpression {
        @Override
        public PValueSource resultValue() {
            return value;
        }

        @Override
        public void evaluate() {
            inputEval.evaluate();
            PValueSource inputVal = inputEval.resultValue();
            cast.evaluate(null, inputVal, value);
        }

        @Override
        public void with(Row row) {
            inputEval.with(row);
        }

        @Override
        public void with(QueryContext context) {
            inputEval.with(context);
        }

        private CastEvaluation(TEvaluatableExpression inputEval, TExecutionContext context, TCast cast) {
            this.context = context;
            this.inputEval = inputEval;
            this.cast = cast;
            this.value = new PValue(context.outputTInstance().typeClass().underlyingType());
        }

        private final TEvaluatableExpression inputEval;
        private final TExecutionContext context;
        private final TCast cast;
        private final PValue value;
    }
}
