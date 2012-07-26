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
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.ArrayList;
import java.util.List;

public final class TInExpression {

    public static TPreparedExpression prepare(TPreparedExpression lhs, List<? extends TPreparedExpression> rhs,
                                              QueryContext queryContext) {
        List<TPreparedExpression> all = new ArrayList<TPreparedExpression>(rhs.size() + 1);
        all.add(lhs);
        all.addAll(rhs);
        return new TPreparedFunction(overload, AkBool.INSTANCE.instance(), all, queryContext);
    }
    
    private static TValidatedOverload overload = new TValidatedOverload(new TOverloadBase() {
        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.vararg(null, 0, 1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            TInstance lhsInstance = context.inputTInstanceAt(0);
            PValueSource lhsSource = inputs.get(0);
            for (int i=1, nInputs = inputs.size(); i < nInputs; ++i) {
                TInstance rhsInstance = context.inputTInstanceAt(i);
                PValueSource rhsSource = inputs.get(i);
                if (0 == TClass.compare(lhsInstance, lhsSource, rhsInstance, rhsSource)) {
                    output.putBool(true);
                    return;
                }
            }
            output.putBool(false);
        }

        @Override
        public String overloadName() {
            return "in";
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.fixed(AkBool.INSTANCE.instance());
        }
    });
}
