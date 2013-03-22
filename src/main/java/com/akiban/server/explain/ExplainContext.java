
package com.akiban.server.explain;

import java.util.HashMap;
import java.util.Map;

public class ExplainContext
{
    private final Map<Explainable,CompoundExplainer> extraInfo = new HashMap<>();

    public ExplainContext() {
    }

    /** Extra info is like debug info in the output from a compiler:
     * information that the optimizer had that helps explain the plan
     * but isn't needed for proper execution. */
    public CompoundExplainer getExtraInfo(Explainable explainable) {
        return extraInfo.get(explainable);
    }

    public boolean hasExtraInfo(Explainable explainable) {
        return extraInfo.containsKey(explainable);
    }

    public void putExtraInfo(Explainable explainable, CompoundExplainer info) {
        CompoundExplainer old = extraInfo.put(explainable, info);
        assert (old == null);
    }
}
