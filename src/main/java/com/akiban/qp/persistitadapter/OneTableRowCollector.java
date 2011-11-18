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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.sun.org.apache.bcel.internal.generic.NEW;

public class OneTableRowCollector extends OperatorBasedRowCollector
{
    OneTableRowCollector(ConfigurationService config,
                         Session session,
                         PersistitStore store,
                         RowDef rowDef,
                         int indexId,
                         int scanFlags,
                         RowData start,
                         ColumnSelector startColumns,
                         RowData end,
                         ColumnSelector endColumns)
    {
        super(store, session, config);
        // rootmostQueryTable
        queryRootTable = rowDef.userTable();
        queryRootType = schema.userTableRowType(queryRootTable);
        requiredUserTables.add(queryRootTable);
        // predicateIndex and predicateType
        predicateIndex = null;
        if (indexId > 0) {
            for (TableIndex userTableIndex : queryRootTable.getIndexesIncludingInternal()) {
                if (userTableIndex.getIndexId() == indexId) {
                    predicateIndex = userTableIndex;
                }
            }
            assert predicateIndex != null : String.format("rowDef: %s, indexId: %s", rowDef, indexId);
        }
        predicateType = queryRootType;
        if (predicateIndex != null) {
            // Index bounds
            IndexRowType indexRowType = schema.indexRowType(predicateIndex);
            ColumnSelector tableSelector;
            if (start == null && end == null) {
                indexKeyRange = new IndexKeyRange(indexRowType);
            } else {
                // The start and end selectors should match.
                assert !(startColumns == null && endColumns == null);
                if (startColumns == null) {
                    tableSelector = endColumns;
                } else if (endColumns == null) {
                    tableSelector = startColumns;
                } else {
                    // Make sure the two selectors match
                    for (int i = 0; i < queryRootTable.getColumns().size(); i++) {
                        assert startColumns.includesColumn(i) == endColumns.includesColumn(i);
                    }
                    tableSelector = startColumns;
                }
                // tableSelector is in terms of table column positions. Need a ColumnSelector based
                // on index column positions.
                ColumnSelector indexSelector = indexSelectorFromTableSelector(predicateIndex, tableSelector);
                IndexBound lo;
                NewRow loRow;
                if (start == null) {
                    loRow = new NiceRow(queryRootTable.getTableId(), (RowDef) queryRootTable.rowDef());
                    for (int i = 0; i < queryRootTable.getColumns().size(); i++) {
                        loRow.put(i, null);
                    }
                } else {
                    assert start.getRowDefId() == queryRootTable.getTableId();
                    loRow = new LegacyRowWrapper(start, store);
                }
                IndexBound hi;
                NewRow hiRow;
                if (end == null) {
                    hiRow = new NiceRow(queryRootTable.getTableId(), (RowDef) queryRootTable.rowDef());
                    for (int i = 0; i < queryRootTable.getColumns().size(); i++) {
                        hiRow.put(i, null);
                    }
                } else {
                    assert end.getRowDefId() == queryRootTable.getTableId();
                    hiRow = new LegacyRowWrapper(end, store);
                }
                indexSelector = ignoreInvalidRestrictions(loRow, hiRow, indexSelector);
                lo = new IndexBound(new NewRowBackedIndexRow(queryRootType, loRow, predicateIndex), indexSelector);
                hi = new IndexBound(new NewRowBackedIndexRow(queryRootType, hiRow, predicateIndex), indexSelector);
                boolean loInclusive = start != null && (scanFlags & (SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_START_EXCLUSIVE)) == 0;
                boolean hiInclusive = end != null && (scanFlags & (SCAN_FLAGS_END_AT_EDGE | SCAN_FLAGS_END_EXCLUSIVE)) == 0;
                indexKeyRange =
                    new IndexKeyRange(indexRowType,
                                      lo,
                                      loInclusive,
                                      hi,
                                      hiInclusive);
            }
        }
    }

    // TODO: EXPERIMENTAL! Used to deal with this problem (from email to Tom):
    /*
The following query is producing an exception from the server:

    select order_id, part_id, unit_price, color, item_status
    from item
    where part_id >= 1400 and color = 'black'

There is an index on (part_id, color), and the execution plan is using
it. The problem is in the IndexKeyRange argument to the
IndexScan_Default operator:

    (>=item(,1400,,,,,,,black),
     <[{item} (0: null) (1: null) (2: null) (3: null) (4: null) (5: null) (6: null) (7: null) (8: null)])

The low and high bounds are in different formats. The low bound is a RowData. The upper bound was actually
supplied as null, and what is shown above is a row containing nulls created in its place.

EXPLAIN says:

mysql> explain select * from item where part_id >= 1400 and color = 'black';
+----+-------------+-------+-------+---------------+--------+---------+------+------+-------------+
| id | select_type | table | type  | possible_keys | key    | key_len | ref  | rows | Extra       |
+----+-------------+-------+-------+---------------+--------+---------+------+------+-------------+
|  1 | SIMPLE      | item  | range | item_2        | item_2 | 27      | NULL |  624 | Using where |
+----+-------------+-------+-------+---------------+--------+---------+------+------+-------------+

The "Using where" tells me that mysql expects to use just the >=1400 part of the index, and that the rest of the
predicate will be applied afterward. (I'm not sure this is a correct inference, and that's really the heart
of the matter.)

The problem is that, according to the new IndexScan implementation, an inequality (part_id >= 1400) must be the
last bounded field of the index. By requesting an equality for a later index column (color = 'black'), we run
afoul of a validity check. This is only a mysql problem. On all other paths, the index scans are being constructed
as described in the referenced document. (HAPI would have the same problem except that it has no syntax for
exploiting a two-column index.) All non-mtr tests are running cleanly.

I can think of a few ways to proceed:

1) Adapter recognizes that the index scan request is not valid and drops the color = 'black' part of the request.

2) Server does the same thing, before creating the execution plan.

3) During query execution, use as much of the index scan request as is valid. When we actually need to open the
cursor on behalf of the IndexScan_Default operator, we will drop the color = 'black' part of the request.

4) Like #3, but anticipating the possibility of the problem, the execution plan includes a Select_HKeyOrdered
operator to check the predicate, just in case.

1-3 all have the same problem, which is that part of the request is being ignored, which will lead to
erroneous results. So here's the question: What is MySQL expecting when it asks the storage engine to do
an index scan of the form we have here? Is it really expecting that restrictions past the first inequality
are going to be applied? If so, that points to #4.

If the answer is no, then my preference is for 1 or 2. I think that 2 might be simpler, and I would try to
keep the logic in the planning phase, so that the index scan operator doesn't have to deal with it. There is
already a large amount of complexity around null handling.

     */
    private ColumnSelector ignoreInvalidRestrictions(NewRow lo, NewRow hi, ColumnSelector indexSelector)
    {
        ColumnSelector narrowedSelector;
        assert hi.getRowDef() == lo.getRowDef();
        int inequalityPosition = -1;
        int nIndexColumns = predicateIndex.getColumns().size();
        for (int indexPosition = 0; inequalityPosition == -1 && indexPosition < nIndexColumns; indexPosition++) {
            IndexColumn indexColumn = predicateIndex.getColumns().get(indexPosition);
            if (indexSelector.includesColumn(indexPosition)) {
                Integer tablePosition = indexColumn.getColumn().getPosition();
                Object loField = lo.get(tablePosition);
                Object hiField = hi.get(tablePosition);
                boolean inequality =
                    loField != hiField &&
                    (loField == null || hiField == null || !loField.equals(hiField));
                if (inequality) {
                    inequalityPosition = indexPosition;
                }
            }
        }
        if (inequalityPosition == -1) {
            narrowedSelector = indexSelector;
        } else {
            final int lastIndexablePosition = inequalityPosition;
            narrowedSelector = new ColumnSelector()
            {
                @Override
                public boolean includesColumn(int columnPosition)
                {
                    return columnPosition <= lastIndexablePosition;
                }
            };
        }
        return narrowedSelector;
    }
}
