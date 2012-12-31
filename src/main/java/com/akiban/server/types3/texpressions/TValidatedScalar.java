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

import com.akiban.server.explain.*;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.ReversedLazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.List;

public final class TValidatedScalar extends TValidatedOverload implements TScalar {

    // TOverload methods (straight delegation)

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs) {
        return scalar.evaluateConstant(context, inputs);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        scalar.finishPreptimePhase(context);
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        scalar.evaluate(context, inputs, output);
    }

    @Override
    public String toString(List<? extends TPreparedExpression> inputs, TInstance resultType) {
        return scalar.toString(inputs, resultType);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        return scalar.getExplainer(context, inputs, resultType);
    }

    public <T> LazyList<? extends T> filterInputs(LazyList<? extends T> inputs) {
        return commuted ? new ReversedLazyList<T>(inputs) : inputs;
    }

    public TValidatedScalar createCommuted() {
        if (!coversExactlyNArgs(2))
            throw new IllegalStateException("commuted overloads must take exactly two arguments: " + this);
        TClass origArg1 = inputSetAt(1).targetType();
        TClass origArg0 = inputSetAt(0).targetType();
        if (origArg0 == origArg1)
            throw new IllegalStateException("two-arg overload has same target class for both operands, so commuting "
                    + "it makes no sense: " + this);

        TInputSetBuilder builder = new TInputSetBuilder();
        builder.covers(origArg1, 0);
        builder.covers(origArg0, 1);
        List<TInputSet> commutedInputSets = builder.toList();
        return new TValidatedScalar(scalar, commutedInputSets, true);
    }

    public TValidatedScalar(TScalar scalar) {
        this(scalar, scalar.inputSets(), false);
    }

    private TValidatedScalar(TScalar scalar, List<TInputSet> inputSets, boolean commuted) {
        super(scalar, inputSets);
        this.commuted = commuted;
        this.scalar = scalar;
    }

    private final TScalar scalar;
    private final boolean commuted;
}
