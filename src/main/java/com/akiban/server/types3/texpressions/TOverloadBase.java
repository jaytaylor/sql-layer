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
import com.akiban.server.types3.LazyListBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.List;

public abstract class TOverloadBase implements TOverload {

    @Override
    public List<TInputSet> inputSets() {
        TInputSetBuilder builder = new TInputSetBuilder();
        buildInputSets(builder);
        return builder.toList();
    }

    @Override
    public void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (nullContaminates(i) && inputs.get(i).isNull()) {
                output.putNull();
            }
        }
        doEvaluate(context, inputs, output);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
    }

    @Override
    public TPreptimeValue evaluateConstant(TPreptimeContext context, final LazyList<? extends TPreptimeValue> inputs) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (constnessMatters(i)) {
                TPreptimeValue preptimeValue = inputs.get(i);
                PValueSource prepSource = (preptimeValue == null) ? null : preptimeValue.value();
                Constantness constness = constness(i, prepSource);
                if (constness == Constantness.NOT_CONST)
                    return null;
                if (constness == Constantness.CONST)
                    break;
                assert constness == Constantness.UNKNOWN : constness;
            }
        }

        // at this point, assume there's a constant value and we can evaluate it

        finishPreptimePhase(context);

        TExecutionContext execContext = context.createExecutionContext();
        LazyList<PValueSource> inputValues = new LazyListBase<PValueSource>() {
            @Override
            public PValueSource get(int i) {
                TPreptimeValue ptValue = inputs.get(i);
                PValueSource source = ptValue.value();
                assert source != null : "non-constant value where constant value expected";
                return source;
            }

            @Override
            public int size() {
                return inputs.size();
            }
        };
        PValue outputValue = new PValue(execContext.outputTInstance().typeClass().underlyingType());
        evaluate(execContext, inputValues, outputValue);
        return new TPreptimeValue(execContext.outputTInstance(), outputValue);
    }

    protected abstract void buildInputSets(TInputSetBuilder builder);

    protected abstract void doEvaluate(TExecutionContext context,
                                       LazyList<? extends PValueSource> inputs,
                                       PValueTarget output);

    protected boolean constnessMatters(int inputIndex) {
        return true;
    }

    /**
     * <p>Determines this expression's constness given information about its input's prepare-time value. If the
     * prepare-time value is known (ie, is a constant), preptimeValue will contain that value. Otherwise, preptimeValue
     * will be <tt>null</tt>. Note that the reference itself will be null, <em>not</em> a reference to a PValueSource
     * whose {@link PValueSource#isNull()} returns <tt>true</tt>. A non-null PValueSource which <tt>isNull</tt> means 
     * a prepare-time constant value of <tt>NULL</tt>.</p>
     * 
     * <p>The default is to return {@linkplain Constantness#NOT_CONST} as soon as a non-const value is seen, and
     * {@linkplain Constantness#UNKNOWN} until then.</p>
     * @param inputIndex the index of the input whose prepare-time value is being considered
     * @param preptimeValue the prepare-time value, or <tt>null</tt> if not a prepare-time constant
     * @return what is known about this expression's constness due to this input
     */
    protected Constantness constness(int inputIndex, PValueSource preptimeValue) {
        return preptimeValue == null ? Constantness.NOT_CONST : Constantness.UNKNOWN;
    }

    protected boolean nullContaminates(int inputIndex) {
        return true;
    }

    @Override
    public String toString() {
        return overloadName() + ": " + inputSets();
    }
}
