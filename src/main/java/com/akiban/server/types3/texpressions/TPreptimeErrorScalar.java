
package com.akiban.server.types3.texpressions;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.util.List;

public abstract class TPreptimeErrorScalar extends TScalarBase {
    protected abstract InvalidOperationException error();

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        for (int pos=0; pos < inputs.length; ++pos) {
            builder.covers(inputs[pos], pos);
        }
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        InvalidOperationException ioe = error();
        assert ioe != null : "no exception provided, but one was expected";
        throw ioe;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Constantness constness(int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return Constantness.NOT_CONST;
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                throw new UnsupportedOperationException();
            }
        });
    }

    protected TPreptimeErrorScalar(int priority, String name, TClass... inputs) {
        this.priority = priority;
        this.name = name;
        this.inputs = inputs;
    }

    private final int priority;
    private final String name;
    private final TClass[] inputs;
}
