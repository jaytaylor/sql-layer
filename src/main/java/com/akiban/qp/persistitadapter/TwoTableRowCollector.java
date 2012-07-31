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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ByteArrayColumnSelector;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataExtractor;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;

public class TwoTableRowCollector extends OperatorBasedRowCollector
{
    TwoTableRowCollector(ConfigurationService config,
                         Session session,
                         PersistitStore store,
                         RowDef rowDef,
                         int indexId,
                         int scanFlags,
                         RowData start,
                         ColumnSelector startColumns,
                         RowData end,
                         ColumnSelector endColumns,
                         byte[] columnBitMap)
    {
        super(store, session, config);
        // Get group table and check that start/end refer to it
        GroupTable groupTable = rowDef.groupTable();
        assert start == null || start.getRowDefId() == groupTable.getTableId() : start;
        assert end == null || end.getRowDefId() == groupTable.getTableId() : end;
        // Analyses based on group columns
        NiceRow userTableStart = null;
        NiceRow userTableEnd = null;
        UserTable predicateTable = null;
        int columnPosition = 0;
        int columnBitMapSize = columnBitMap.length * 8;
        RowDataExtractor startExtractor = start == null ? null : new RowDataExtractor(start, rowDef);
        RowDataExtractor endExtractor = end == null ? null : new RowDataExtractor(end, rowDef);
        while (columnPosition < columnBitMapSize) {
            if ((columnBitMap[columnPosition / 8] & (1 << (columnPosition % 8))) != 0) {
                Column groupColumn = groupTable.getColumnsIncludingInternal().get(columnPosition);
                Column userColumn = groupColumn.getUserColumn();
                UserTable userTable = userColumn.getUserTable();
                requiredUserTables.add(userTable);
                // rootmostQueryTable
                if (queryRootTable == null) {
                    queryRootTable = userTable;
                }
                // predicateTable, predicateRowDef, start/end translated to UserTable rows.
                if (start != null && startColumns.includesColumn(columnPosition) ||
                    end != null && endColumns.includesColumn(columnPosition)) {
                    if (predicateTable == null) {
                        predicateTable = userTable;
                        userTableStart = start == null ? null : new NiceRow(predicateTable.getTableId(), store);
                        userTableEnd = end == null ? null : new NiceRow(predicateTable.getTableId(), store);
                    } else if (predicateTable != userTable) {
                        throw new IllegalArgumentException
                            (String.format("Restriction on at least two tables: %s, %s",
                                           predicateTable, userTable));
                    }
                    FieldDef fieldDef = rowDef.getFieldDef(groupColumn.getPosition());
                    if (userTableStart != null) {
                        userTableStart.put(userColumn.getPosition(), startExtractor.get(fieldDef));
                    }
                    if (userTableEnd != null) {
                        userTableEnd.put(userColumn.getPosition(), endExtractor.get(fieldDef));
                    }
                }
            }
            columnPosition++;
        }
        assert queryRootTable != null;
        queryRootType = schema.userTableRowType(queryRootTable);
        if (predicateTable != null) {
            // predicateIndex and predicateType
            predicateIndex = null;
            for (TableIndex userTableIndex : predicateTable.getIndexesIncludingInternal()) {
                if (userTableIndex.getIndexId() == indexId) {
                    predicateIndex = userTableIndex;
                }
            }
            assert predicateIndex != null : String.format("table %s, index %s", predicateTable, indexId);
            predicateType = schema.userTableRowType((UserTable) predicateIndex.getTable());
            // Index bounds
            IndexBound lo =
                userTableStart == null
                ? null
                : new IndexBound(new NewRowBackedIndexRow(predicateType, userTableStart, predicateIndex),
                                 indexSelectorFromTableSelector(predicateIndex, userColumnSelector(predicateTable,
                                                                                                   startColumns)));
            IndexBound hi =
                userTableEnd == null
                ? null
                : new IndexBound(new NewRowBackedIndexRow(predicateType, userTableEnd, predicateIndex),
                                 indexSelectorFromTableSelector(predicateIndex, userColumnSelector(predicateTable,
                                                                                                   endColumns)));
            IndexRowType indexRowType = schema.indexRowType(predicateIndex);
            indexKeyRange = IndexKeyRange.bounded(indexRowType,
                                                  lo,
                                                  lo != null && (scanFlags & (SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_START_EXCLUSIVE)) == 0,
                                                  hi,
                                                  hi != null && (scanFlags & (SCAN_FLAGS_END_AT_EDGE | SCAN_FLAGS_END_EXCLUSIVE)) == 0);
        }
    }

    private ColumnSelector userColumnSelector(UserTable table, ColumnSelector groupColumnSelector)
    {
        int nColumns = table.getColumnsIncludingInternal().size();
        byte[] columnBitMap = new byte[(nColumns + 7) / 8];
        for (Column userColumn : table.getColumnsIncludingInternal()) {
            Column groupColumn = userColumn.getGroupColumn();
            if (groupColumnSelector.includesColumn(groupColumn.getPosition())) {
                int p = userColumn.getPosition();
                columnBitMap[p / 8] |= 1 << (p % 8);
            }
        }
        return new ByteArrayColumnSelector(columnBitMap);
    }
}
