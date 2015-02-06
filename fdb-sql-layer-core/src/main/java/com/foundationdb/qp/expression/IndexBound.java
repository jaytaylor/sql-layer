/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.expression;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.types.value.ValueRecord;

public class IndexBound
{
    public String toString()
    {
        return String.valueOf(unboundExpressions);
    }

    public ValueRecord boundExpressions(QueryContext context, QueryBindings bindings)
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

    public IndexBound(ValueRecord row, ColumnSelector columnSelector)
    {
        this(new PreBoundExpressions(row), columnSelector);
    }

    public IndexBound(UnboundExpressions unboundExpressions, ColumnSelector columnSelector)
    {
        this.unboundExpressions = unboundExpressions;
        this.columnSelector = columnSelector;
    }
    
    public boolean isLiteralNull(int index) {
        return unboundExpressions.isLiteralNull(index);
    }

    // Object state

    private final UnboundExpressions unboundExpressions;
    private final ColumnSelector columnSelector;

    // nested classes

    private static class PreBoundExpressions implements UnboundExpressions {

        @Override
        public ValueRecord get(QueryContext context, QueryBindings bindings) {
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
        
        @Override 
        public boolean isLiteralNull(int index) {
            // Because this is built off a row queried from the database, 
            // none of the values can ever be a literal null.
            return false;
        }

        public PreBoundExpressions(ValueRecord expressions) {
            this.expressions = expressions;
        }

        private final ValueRecord expressions;
    }
}
