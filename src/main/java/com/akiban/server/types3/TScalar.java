
package com.akiban.server.types3;

import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.util.List;

public interface TScalar extends TOverload {
    TPreptimeValue evaluateConstant(TPreptimeContext context, LazyList<? extends TPreptimeValue> inputs);
    void finishPreptimePhase(TPreptimeContext context);
    void evaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output);
    String toString(List<? extends TPreparedExpression> inputs, TInstance resultType);
    CompoundExplainer getExplainer(ExplainContext context, List<? extends TPreparedExpression> inputs, TInstance resultType);
}
