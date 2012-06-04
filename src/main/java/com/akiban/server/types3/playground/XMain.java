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

import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedFunction;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.Arrays;

public final class XMain {
    public static void main(String[] args) throws InterruptedException {
        TPreparedExpression literal3 = new TPreparedLiteral(XInt.INSTANCE, pvalue32(3));
        TPreparedExpression literal5 = new TPreparedLiteral(XInt.INSTANCE, pvalue32(5));

        TValidatedOverload validatedAdd = new TValidatedOverload(new XAddInt());

        TPreparedExpression preparedExpression = new TPreparedFunction(
                validatedAdd,
                XInt.INSTANCE,
                Arrays.asList(literal3, literal5));
        preparedExpression = new TPreparedFunction(
                validatedAdd,
                XInt.INSTANCE,
                Arrays.asList(preparedExpression, new XIntTime())
        );

        TPreptimeValue preptimeValue = preparedExpression.evaluateConstant();
        if (preptimeValue != null && preptimeValue.value() != null) {
            System.out.println("constant");
            System.out.println(preptimeValue.value());
        }
        else {
            new SimpleQueryContext(null); // force the class loader, so we don't pay for it within the loop
            System.out.println("non-constant:");
            for (int i = 0; i < 10; ++i) {
                TEvaluatableExpression eval = preparedExpression.build();
                eval.with(new SimpleQueryContext(null));
                eval.evaluate();
                System.out.println(eval.resultValue());
                Thread.sleep(5);
            }
        }
    }

    private static PValueSource pvalue32(int value) {
        PValue pvalue = new PValue(PUnderlying.INT_32);
        pvalue.putInt32(value);
        return pvalue;
    }
}
