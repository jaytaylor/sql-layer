
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

/**
 * <p>A function for describing the prepare-time type information of other expression. If/when we divide bundles into
 * modular packages, this should go into a testing or debug package. Its primary purpose, at least for now, is to verify
 * in our yaml tests that expressions have the right type.</p>
 *
 * <p>The usage is <code>DESCRIBE_EXPRESSION(<i>expr</i>)</code>, and the result is a constant {@code VARCHAR(255)} which
 * describes the TInstance and constantness of <i>expr</i>.</p>
 */
public final class DescribeExpression extends TScalarBase {

    public static final TScalar instance = new DescribeExpression();

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(null, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String result = context.inputTInstanceAt(0).toString();
        PValueSource input = inputs.get(0);
        result = ( (input== null) ? "variable " : "const ") + result;
        output.putString(result, null);
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    protected Constantness constness(int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return Constantness.CONST;
    }

    @Override
    protected boolean allowNonConstsInEvaluation() {
        return true;
    }

    @Override
    public String displayName() {
        return "describe_expression";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MString.VARCHAR, 255);
    }

    private DescribeExpression() {}
}
