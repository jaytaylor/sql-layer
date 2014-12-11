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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.explain.*;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**

 <h1>Overview</h1>

 IndexScan_Default scans an index to locate index records whose keys
 are inside a given range.

 <h1>Arguments</h1>


 <ul>

 <li><b>IndexRowType indexType:</b> The index's type.

 <li><b>IndexKeyRange indexKeyRange:</b> Describes the range of keys
 to be visited. The values specified by the indexKeyRange should
 restrict one or more of the leading fields of the index. If null,
 then the entire index will be scanned.

 <li><b>Ordering ordering:</b> Indicates whether keys should be visited
  in ascending order or descending order.

 <li><b>IndexScanSelector scanSelector:</b> On a group index, specify which
  tables must be present for OUTER JOIN semantics.

 <li><b>int lookaheadQuantum:</b> Number of cursors to try to keep open by looking
  ahead in bindings stream.

 </ul>

 <h1>Behavior</h1>

 If reverse = false, then the index is probed using the low end of the
 indexKeyRange. Index records are written to the output stream as long
 as they fall inside the indexKeyRange. When the first record outside
 the indexKeyRange is located, the scan is closed.

 If reverse = true, the initial probe is with the high end of the
 indexKeyRange, and records are visited in descending key order.

 innerJoinUntilRowType is the table until which a group index is
 treated with INNER JOIN semantics (inclusive). For instance, let's say
 you had a COI schema with group index (customer.name,
 order.date). The group table has the following rows:

 <table>
 <tr><td>Row</td></tr>
 <tr><td>c(1, Bob)</td></tr>
 <tr><tr>c(2, Joe)</td></tr>
 <tr><tr>o(10, 2, 01-01-2001)</td></tr>
 <tr><tr>o(11, 3, null)</td></tr>
 </table>

 This corresponds to the following rows in the group index, which has
 LEFT JOIN semantics:

 <table>
 <tr><td>Key</td><td>Value</td><td>Notes</td></tr>
 <tr><td>Bob, null, hkey(c1)</td><td>depth(c)</td><td>null o.date is due to there not being any child orders</td></tr>
 <tr><td>Joe, null, hkey(o10)</td><td>depth(o)</td><td>null o.date is due to o(10) having a null o.date</td></tr>
 <tr><td>Joe, 01-01-2001, hkey(o11)</td><td>depth(o)</td><td></td></tr>
 </table>

 If we're executing a query which has a LEFT JOIN between c and o, we
 would pass tableRowType(CUSTOMER) as the innerJoinUntilRowType,
 and get all of those rows. If we were executing a query plan which had
 an INNER JOIN between c and o, we would pass tableRowType(ORDER)
 and get only the second two rows (with depth(o) ).

 Notes:
 <ul>

 <li>it's possible to specify INNER only partially up the branch. For
 instance, if our group index had been on (customer.name, order.date,
 item.sku), passing tableRowType(ORDER) would be analogous to SQL
 FROM c INNER JOIN o LEFT JOIN i.

 <li>specifying the TableRowType corresponding to the group
 index's rootmost table means the index will be scanned only with
 LEFT JOIN semantics; all entries (within the key range) will be
 returned.

 <li>specifying a TableRowType not within the group index's
 branch segment (i.e: rootward of the GI's rootmost table; or
 leaftward of the GI's leafmost table; or in another branch or group)
 will result in an IllegalArgumentException during the
 PhysicalOperator's construction

 </ul>

 <h1>Output</h1>

 Output contains index rows. Each row has an hkey of the index's table.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 IndexScan_Default does one random access followed by as many sequential accesses as are required to cover the indexKeyRange.

 <h1>Memory Requirements</h1>

 None.

 */

class IndexScan_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(").append(index);
        str.append(" ").append(indexKeyRange);
        if (!ordering.allAscending()) {
            str.append(" ").append(ordering);
        }
        str.append(scanSelector.describe());
        str.append(")");
        return str.toString();
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        if (lookaheadQuantum <= 1) {
            return new Execution(context, bindingsCursor);
        }
        else {
            return new LookaheadExecution(context, bindingsCursor, 
                                          context.getStore(index.rootMostTable()),
                                          lookaheadQuantum);
        }
    }

    // IndexScan_Default interface

    public IndexScan_Default(IndexRowType indexType,
                             IndexKeyRange indexKeyRange,
                             API.Ordering ordering,
                             IndexScanSelector scanSelector,
                             int lookaheadQuantum)
    {
        ArgumentValidation.notNull("indexType", indexType);
        this.indexType = indexType;
        this.index = indexType.index();
        this.ordering = ordering;
        this.indexKeyRange = indexKeyRange;
        this.scanSelector = scanSelector;
        this.lookaheadQuantum = lookaheadQuantum;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: IndexScan_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: IndexScan_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(IndexScan_Default.class);

    // Object state

    private final IndexRowType indexType;
    private final Index index;
    private final API.Ordering ordering;
    private final IndexKeyRange indexKeyRange;
    private final IndexScanSelector scanSelector;
    private final int lookaheadQuantum;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.INDEX, indexType.getExplainer(context));
        for (IndexColumn indexColumn : index.getAllColumns()) {
            Column column = indexColumn.getColumn();
            atts.put(Label.TABLE_SCHEMA, PrimitiveExplainer.getInstance(column.getTable().getName().getSchemaName()));
            atts.put(Label.TABLE_NAME, PrimitiveExplainer.getInstance(column.getTable().getName().getTableName()));
            atts.put(Label.COLUMN_NAME, PrimitiveExplainer.getInstance(column.getName()));
        }
        if (index.isGroupIndex())
            atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("GROUP"));
        if (!indexKeyRange.unbounded()) {
            List<Explainer> loExprs = null, hiExprs = null;
            if (indexKeyRange.lo() != null) {
                loExprs = indexKeyRange.lo().getExplainer(context).get().get(Label.EXPRESSIONS);
            }
            if (indexKeyRange.hi() != null) {
                hiExprs = indexKeyRange.hi().getExplainer(context).get().get(Label.EXPRESSIONS);
            }
            if (indexKeyRange.spatialCoordsIndex()) {
                if (index.isGroupIndex()) {
                    atts.remove(Label.INDEX_KIND);
                    atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("SPATIAL GROUP"));
                } else {
                    atts.put(Label.INDEX_KIND, PrimitiveExplainer.getInstance("SPATIAL"));
                }
                int nequals = indexKeyRange.boundColumns() - index.spatialColumns();
                if (nequals > 0) {
                    for (int i = 0; i < nequals; i++) {
                        atts.put(Label.EQUAL_COMPARAND, loExprs.get(i));
                    }
                    loExprs = loExprs.subList(nequals, loExprs.size());
                    if (hiExprs != null) {
                        hiExprs = hiExprs.subList(nequals, hiExprs.size());
                    }
                }
                atts.put(Label.LOW_COMPARAND, loExprs);
                if (hiExprs != null) {
                    atts.put(Label.HIGH_COMPARAND, hiExprs);
                }
            }
            else {
                int boundColumns = indexKeyRange.boundColumns();
                for (int i = 0; i < boundColumns; i++) {
                    boolean equals = ((i < boundColumns-1) ||
                                      ((loExprs != null) && (hiExprs != null) &&
                                       indexKeyRange.loInclusive() && indexKeyRange.hiInclusive() &&
                                       loExprs.get(i).equals(hiExprs.get(i))));
                    if (equals) {
                        atts.put(Label.EQUAL_COMPARAND, loExprs.get(i));
                    }
                    else {
                        if (loExprs != null) {
                            atts.put(Label.LOW_COMPARAND, loExprs.get(i));
                            atts.put(Label.LOW_COMPARAND, PrimitiveExplainer.getInstance(indexKeyRange.loInclusive()));
                        }
                        if (hiExprs != null) {
                            atts.put(Label.HIGH_COMPARAND, hiExprs.get(i));
                            atts.put(Label.HIGH_COMPARAND, PrimitiveExplainer.getInstance(indexKeyRange.hiInclusive()));
                        }
                    }
                }
            }
        }
        for (int i = 0; i < ordering.sortColumns(); i++) {
            atts.put(Label.ORDERING, PrimitiveExplainer.getInstance(ordering.ascending(i) ? "ASC" : "DESC"));
        }
        atts.put(Label.PIPELINE, PrimitiveExplainer.getInstance(lookaheadQuantum));
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new CompoundExplainer(Type.SCAN_OPERATOR, atts);
    }

    // Inner classes

    private class Execution extends LeafCursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
                cursor.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                checkQueryCancelation();
                Row row = cursor.next();
                if (row == null) {
                    setIdle();
                }
                if (LOG_EXECUTION) {
                    LOG.debug("IndexScan_Default$Execution: yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
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
        public void close() {
            cursor.close();
            super.close();
        }

        @Override
        public QueryBindings nextBindings() {
            QueryBindings bindings = super.nextBindings();
            if (cursor instanceof BindingsAwareCursor)
                ((BindingsAwareCursor)cursor).rebind(bindings);
            return bindings;
        }

        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context, bindingsCursor);
            Table table = index.rootMostTable();
            this.cursor = adapter(table).newIndexCursor(context, indexType, indexKeyRange, ordering, scanSelector, false);
        }

        // Object state

        private final RowCursor cursor;
    }

    private class LookaheadExecution extends LookaheadLeafCursor<BindingsAwareCursor>
    {
        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                Row row = super.next();
                if (LOG_EXECUTION) {
                    LOG.debug(IndexScan_Default.this.toString() + ": yield {}", row);
                }
                return row;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        // LookaheadLeafCursor interface

        @Override
        protected BindingsAwareCursor newCursor(QueryContext context, StoreAdapter adapter) {
            return (BindingsAwareCursor)adapter.newIndexCursor(context, indexType, indexKeyRange, ordering, scanSelector, true);
        }

        @Override
        protected BindingsAwareCursor openACursor(QueryBindings bindings, boolean lookahead) {
            if (LOG_EXECUTION) {
                LOG.debug(IndexScan_Default.this.toString() + ": open{} for {}", lookahead ? " lookahead" : "", bindings);
            }
            return super.openACursor(bindings, lookahead);
        }
        
        // LookaheadExecution interface

        LookaheadExecution(QueryContext context, QueryBindingsCursor bindingsCursor, 
                           StoreAdapter adapter, int quantum) {
            super(context, bindingsCursor, adapter, quantum);
        }
        
        @Override
        public String toString() {
            return "LookaheadExecution for " + IndexScan_Default.this.toString();
        }
    }
}
