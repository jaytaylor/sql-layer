package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.explain.*;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.util.List;

public class MConcat extends TScalarBase {
    public static final TScalar INSTANCE = new MConcat();
    
    private MConcat(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.nextInputPicksWith(MString.VARCHAR.PICK_RIGHT_LENGTH).vararg(MString.VARCHAR, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < inputs.size(); ++i) {
            String inputStr = inputs.get(i).getString();
            assert inputStr != null;
            sb.append(inputStr);
        }
        output.putString(sb.toString(), null);
    }

    @Override
    public String displayName() {
        return "concatenate";
    }

    @Override
    public String[] registeredNames() {
        return new String[] { "concatenate", "concat" };
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                int length = 0;
                for (TPreptimeValue ptv : inputs) {
                    length += ptv.instance().attribute(StringAttribute.MAX_LENGTH);
                }
                return MString.VARCHAR.instance(length, anyContaminatingNulls(inputs));
            }
        });
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType)
    {
        CompoundExplainer ex = super.getExplainer(context, inputs, resultType);
        ex.addAttribute(Label.INFIX_REPRESENTATION, PrimitiveExplainer.getInstance("||"));
        ex.addAttribute(Label.ASSOCIATIVE, PrimitiveExplainer.getInstance(true));
        return ex;
    }
}
