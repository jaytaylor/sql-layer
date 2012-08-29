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
