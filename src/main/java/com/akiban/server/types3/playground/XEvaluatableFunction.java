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

package com.akiban.server.types3.playground;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

import java.util.ArrayList;
import java.util.List;

public final class XEvaluatableFunction implements XEvaluatableExpression {

    @Override
    public TInstance resultType() {
        return resultType;
    }

    @Override
    public PValueSource resultValue() {
        return resultValue;
    }

    @Override
    public void evaluate() {
        overload.overload().evaluate(inputTypes, evaluations, resultType, resultValue);
    }

    public XEvaluatableFunction(XValidatedOverload overload, TInstance resultType,
                                List<? extends XEvaluatableExpression> inputs) {
        this.overload = overload;
        this.inputs = inputs;
        this.inputValues = new PValueSource[inputs.size()];
        this.resultType = resultType;
        resultValue = new PValue(resultType.typeClass().underlyingType());
        this.inputTypes = new ArrayList<TInstance>(inputs.size());
        for (XEvaluatableExpression input : inputs) {
            inputTypes.add(input.resultType());
        }
    }

    private final XValidatedOverload overload;
    private final PValueSource[] inputValues;
    private final List<TInstance> inputTypes;
    private final List<? extends XEvaluatableExpression> inputs;
    private final TInstance resultType;
    private final PValue resultValue;

    private final LazyList<PValueSource> evaluations = new LazyList<PValueSource>() {
        @Override
        public PValueSource get(int i) {
            PValueSource value = inputValues[i];
            if (value == null) {
                XEvaluatableExpression inputExpr = inputs.get(i);
                inputExpr.evaluate();
                value = inputExpr.resultValue();
                inputValues[i] = value;
            }
            return value;
        }
    };
}
