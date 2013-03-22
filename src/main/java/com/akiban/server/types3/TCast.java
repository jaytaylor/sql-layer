
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

public interface TCast {
    Constantness constness();
    public TClass sourceClass();
    public TClass targetClass();
    public TInstance preferredTarget(TPreptimeValue source);

    public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target);
}
