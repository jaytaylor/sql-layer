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
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

abstract class ContextualEvaluation<T> implements TEvaluatableExpression {

    @Override
    public void with(Row row) {
    }

    @Override
    public void with(QueryContext context) {
    }

    @Override
    public PValueSource resultValue() {
        if (readyValue == null)
            throw new IllegalStateException("haven't evaluated since having seen a new context");
        return readyValue;
    }

    @Override
    public void evaluate() {
        if (context == null)
            throw new IllegalStateException("no context given");
        if (readyValue == null) {
            if (unreadyValue == null) {
                // first evaluation of this expression
                readyValue = new PValue(underlyingType);
            }
            else {
                // readyValue is null, unreadyValue is not null. Means we've seen a QueryContext but have
                // not evaluated it. Set the readyValue to unreadyValue, as we've about to evaluate it.
                readyValue = unreadyValue;
                unreadyValue = null;
            }
        }
        evaluate(context, readyValue);
    }

    protected void setContext(T context) {
        if (unreadyValue == null) {
            // give unreadValue the readyValue, whatever it is (it could be null if we've never evaluated)
            // then set readyValue to null
            unreadyValue = readyValue;
            readyValue = null;
        }
        this.context = context;
    }
    
    protected abstract void evaluate(T context, PValueTarget target);

    protected ContextualEvaluation(PUnderlying underlyingType) {
        this.underlyingType = underlyingType;
    }

    // At most one of these will be non-null. If no QueryContext has been seen yet, they'll both be null.
    // Otherwise, unreadyValue means we've seen but not evaluated a QueryContext, and readyValue means we've
    // seen a QueryContext and evaluated it.
    private PValue unreadyValue;
    private PValue readyValue;
    private T context;
    private PUnderlying underlyingType;
}
