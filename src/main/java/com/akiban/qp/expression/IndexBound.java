/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.expression;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ColumnSelector;

public class IndexBound
{
    public String toString()
    {
        return String.valueOf(unboundExpressions);
    }

    public BoundExpressions boundExpressions(Bindings bindings, StoreAdapter adapter)
    {
        return unboundExpressions.get(bindings, adapter);
    }

    public ColumnSelector columnSelector()
    {
        return columnSelector;
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
        public BoundExpressions get(Bindings bindings, StoreAdapter adapter) {
            return expressions;
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
