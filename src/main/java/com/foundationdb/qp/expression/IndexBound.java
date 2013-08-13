/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.qp.expression;

import com.akiban.qp.operator.QueryBindings;
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

    public BoundExpressions boundExpressions(QueryContext context, QueryBindings bindings)
    {
        return unboundExpressions.get(context, bindings);
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
        public BoundExpressions get(QueryContext context, QueryBindings bindings) {
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
