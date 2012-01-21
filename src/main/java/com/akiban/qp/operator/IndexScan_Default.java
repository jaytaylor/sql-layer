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

package com.akiban.qp.operator;

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 <h1>Overview</h1>

 IndexScan_Default scans an index to locate index records whose keys
 are inside a given range.

 <h1>Arguments</h1>


 <ul>

 <li><b>IndexRowType indexType:</b> The index's type.

 <li><b>boolean reverse:</b> Indicates whether keys should be visited
 in ascending order (reverse = false) or descending order (reverse =
 true).

 <li><b>IndexKeyRange indexKeyRange:</b> Describes the range of keys
 to be visited. The values specified by the indexKeyRange should
 restrict one or more of the leading fields of the index. If null,
 then the entire index will be scanned.

 <li><b>UserTableRowType innerJoinUntilRowType</b>: On a table index,
 this must be the UserTableRowType of the Index's table (but it's
 ignored). On a group index, this is the table until which the group
 index is interpreted with INNER JOIN semantics. The specified row
 type must be within the group index's branch segment.

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
 would pass userTableRowType(CUSTOMER) as the innerJoinUntilRowType,
 and get all of those rows. If we were executing a query plan which had
 an INNER JOIN between c and o, we would pass userTableRowType(ORDER)
 and get only the second two rows (with depth(o) ).

 Notes:
 <ul>

 <li>it's possible to specify INNER only partially up the branch. For
 instance, if our group index had been on (customer.name, order.date,
 item.sku), passing userTableRowType(ORDER) would be analogous to SQL
 FROM c INNER JOIN o LEFT JOIN i.

 <li>specifying the UserTableRowType corresponding to the group
 index's rootmost table means the index will be scanned only with
 LEFT JOIN semantics; all entries (within the key range) will be
 returned.

 <li>specifying a UserTableRowType not within the group index's
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
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    // IndexScan_Default interface

    public IndexScan_Default(IndexRowType indexType,
                             IndexKeyRange indexKeyRange,
                             API.Ordering ordering,
                             IndexScanSelector scanSelector)
    {
        ArgumentValidation.notNull("indexType", indexType);
        this.index = indexType.index();
        this.ordering = ordering;
        this.indexKeyRange = indexKeyRange;
        this.scanSelector = scanSelector;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexScan_Default.class);
    private static final Tap.PointTap INDEX_SCAN_COUNT = Tap.createCount("operator: index_scan", true);

    // Object state

    private final Index index;
    private final API.Ordering ordering;
    private final IndexKeyRange indexKeyRange;
    private final IndexScanSelector scanSelector;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // OperatorExecution interface

        // Cursor interface

        @Override
        public void open()
        {
            INDEX_SCAN_COUNT.hit();
            cursor.open();
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row = cursor.next();
            if (row == null) {
                close();
            } else {
                row.runId(runIdCounter++);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("IndexScan: yield {}", row);
            }
            return row;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.cursor = adapter().newIndexCursor(context, index, indexKeyRange, ordering, scanSelector);
        }

        // Object state

        private final Cursor cursor;
        private int runIdCounter = 0;
    }
}
