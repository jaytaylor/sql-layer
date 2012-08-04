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
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PUnderlying;
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
        return new Evaluation(typeInstance.typeClass().underlyingType(), fieldIndex);
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

        private Evaluation(PUnderlying underlyingType, int fieldIndex) {
            super(underlyingType);
            this.fieldIndex = fieldIndex;
        }

        private int fieldIndex;
    }
}
