
package com.akiban.qp.expression;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.CompoundExplainer;

public class IndexBound
{
    public String toString()
    {
        return String.valueOf(unboundExpressions);
    }

    public BoundExpressions boundExpressions(QueryContext context)
    {
        return unboundExpressions.get(context);
    }

    public ColumnSelector columnSelector()
    {
        return columnSelector;
    }

    public CompoundExplainer getExplainer(ExplainContext context) {
        return unboundExpressions.getExplainer(context);
    }

    public IndexBound(BoundExpressions row, ColumnSelector columnSelector)
    {
        this(new PreBoundExpressions(row), columnSelector);
    }

    public IndexBound(UnboundExpressions unboundExpressions, ColumnSelector columnSelector)
    {
        this.unboundExpressions = unboundExpressions;
        this.columnSelector = columnSelector;
    }

    // Object state

    private final UnboundExpressions unboundExpressions;
    private final ColumnSelector columnSelector;

    // nested classes

    private static class PreBoundExpressions implements UnboundExpressions {

        @Override
        public BoundExpressions get(QueryContext context) {
            return expressions;
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return String.valueOf(expressions);
        }

        public PreBoundExpressions(BoundExpressions expressions) {
            this.expressions = expressions;
        }

        private final BoundExpressions expressions;
    }
}
