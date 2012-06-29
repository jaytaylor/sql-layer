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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.util.SparseArray;

import java.util.List;

public final class TValidatedOverload implements TOverload {

    // TOverload methods (straight delegation)

    @Override
    public String overloadName() {
        return overload.overloadName();
    }

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs) {
        return overload.evaluateConstant(context, inputs);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        overload.finishPreptimePhase(context);
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        overload.evaluate(context, inputs, output);
    }

    // TOverload methods (cached)

    @Override
    public List<TInputSet> inputSets() {
        return inputSetsCached;
    }

    @Override
    public TOverloadResult resultType() {
        return resultStrategy;
    }

    // TValidatedOverload methods

    public int firstVarargInput() {
        if (varargs == null)
            return -1;
        return inputSetsByPos.size();
    }

    public TInputSet pickingInputSet() {
        return pickingSet;
    }

    public TInputSet varargInputSet() {
        return varargs;
    }

    public TInputSet inputSetAt(int index) {
        return inputSetsByPos.get(index);
    }

    public TOverloadResult resultStrategy() {
        throw new UnsupportedOperationException();
    }

    public boolean coversNInputs(int nInputs) {
        /* no pos           : nInputs = 0
         * POS(N)           : nInputs = N+1
         * REMAINING        : nInputs >= 0
         * POS(N),REMAINING : nInputs >= N+1
         */
        if (varargs == null)
            return nInputs == inputSetsByPos.size();
        return nInputs >= varargs.positionsLength();
    }

    public int positionalInputs() {
        throw new UnsupportedOperationException();
    }

    public TValidatedOverload(TOverload overload) {
        TInputSet localVarargInputs = null;
        TInputSet localPickingInputs = null;
        SparseArray<TInputSet> inputSetsArray = new SparseArray<TInputSet>();
        this.inputSetsCached = overload.inputSets();
        for (TInputSet inputSet : inputSetsCached) {
            if (inputSet.coversRemaining()) {
                if (localVarargInputs != null)
                    throw new InvalidOverloadException("multiple input sets are vararg");
                localVarargInputs = inputSet;
            }
            for (int i = 0, max = inputSet.positionsLength(); i < max; ++i) {
                if (inputSet.covers(i)) {
                    if (inputSetsArray.isDefined(i))
                        throw new InvalidOverloadException("multiple input sets cover input " + i);
                    inputSetsArray.set(i, inputSet);
                }
            }
            if (inputSet.isPicking()) {
                if (localPickingInputs != null)
                    throw new InvalidOverloadException("overloads can't define multiple picking input sets");
                localPickingInputs = inputSet;
            }
        }
        if (!inputSetsArray.isCompactable())
            throw new InvalidOverloadException("not all inputs covered");
        if ((localVarargInputs != null) && (localVarargInputs.positionsLength() < inputSetsArray.lastDefinedIndex()))
            throw new InvalidOverloadException("REMAINING cannot overlap any POS");
        this.overload = overload;
        this.inputSetsByPos = inputSetsArray.toList();
        this.varargs = localVarargInputs;
        this.resultStrategy = overload.resultType();
        this.pickingSet = localPickingInputs;
    }

    private boolean stronglyCastable(TClass tClass, TClass tClass1) {
        throw new UnsupportedOperationException(); // TODO
    }
    
    private final TOverload overload;
    private final List<TInputSet> inputSetsCached;
    private final List<TInputSet> inputSetsByPos;
    private final TOverloadResult resultStrategy;
    private final TInputSet varargs;
    private final TInputSet pickingSet;

    private static class InvalidOverloadException extends RuntimeException {
        private InvalidOverloadException(String message) {
            super(message);
        }
    }
}
