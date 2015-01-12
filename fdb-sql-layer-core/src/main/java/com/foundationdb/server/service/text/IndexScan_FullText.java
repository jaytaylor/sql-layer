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

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.IndexName;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.LeafCursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindingsCursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;

import org.apache.lucene.search.Query;

public class IndexScan_FullText extends Operator
{
    private final IndexName index;
    private final FullTextQueryExpression queryExpression;
    private final int limit;
    private final RowType rowType;

    public IndexScan_FullText(IndexName index, 
                              FullTextQueryExpression queryExpression, 
                              int limit,
                              RowType rowType) {
        this.index = index;
        this.queryExpression = queryExpression;
        this.limit = limit;
        this.rowType = rowType;
    }

    @Override
    public RowType rowType() {
        if (rowType != null)
            return rowType;
        else
            return super.rowType(); // Only when testing and not needed.
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, bindingsCursor);
    }
    
    protected class Execution extends LeafCursor {
        private final FullTextIndexService service;
        private RowCursor cursor;

        public Execution(QueryContext context, QueryBindingsCursor bindingsCursor) {
            super(context, bindingsCursor);
            service = context.getServiceManager().getServiceByClass(FullTextIndexService.class);
            if (!queryExpression.needsBindings()) {
                // Can reuse cursor if it doesn't need bindings at open() time.
                Query query = queryExpression.getQuery(context, null);
                cursor = service.searchIndex(context, index, query, limit);
            }
        }

        @Override
        public void open()
        {
            super.open();
            if (queryExpression.needsBindings()) {
                Query query = queryExpression.getQuery(context, bindings);
                cursor = service.searchIndex(context, index, query, limit);
            }
            cursor.open();
        }

        @Override
        public Row next()
        {
            if (CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(this);
            }
            checkQueryCancelation();
            return cursor.next();
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector)
        {
            if (CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(this);
            }
            cursor.jump(row, columnSelector);
            state = CursorLifecycle.CursorState.ACTIVE;
        }

        @Override
        public void close()
        {
            try {
                if (cursor != null) {
                    cursor.close();
                    cursor = null;
                /*
                if (queryExpression.needsBindings()) {
                    cursor = null;
                }
                else {
                    cursor.close();
                }
                */
                }
            } finally {
                super.close();
            }
        }

        @Override
        public boolean isIdle()
        {
            return (cursor == null) ? super.isIdle() : cursor.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return (cursor != null) && cursor.isActive();
        }

        @Override
        public boolean isClosed()
        {
            return (cursor == null) ? super.isClosed() : cursor.isClosed();
        }
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INDEX, PrimitiveExplainer.getInstance(index.toString()));
        atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("FULL_TEXT"));
        atts.put(Label.PREDICATE, queryExpression.getExplainer(context));
        if (limit > 0)
            atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new CompoundExplainer(Type.SCAN_OPERATOR, atts);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getName());
        str.append("(").append(index);
        str.append(" ").append(queryExpression);
        if (limit > 0) {
            str.append(" LIMIT ").append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
