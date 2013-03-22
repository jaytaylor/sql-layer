
package com.akiban.server.explain;

public interface Explainable
{
    CompoundExplainer getExplainer(ExplainContext context);
}
