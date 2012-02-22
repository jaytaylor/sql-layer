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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import com.akiban.ais.model.*;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;

import java.util.*;

public class RowDataGenealogist implements Genealogist<RowData>
{
    // Genealogist interface

    @Override
    public void fillInMissing(RowData previousRow, RowData row, Queue<RowData> missingRows)
    {
        int differsAt = row.differsFromPredecessorAtKeySegment();
        UserTable rootmostMissingAncestor =
            differsAt == 0
            ? queryRoot
            : hKeyBoundaries.get(row.getRowDefId())[differsAt];
        if (rootmostMissingAncestor.getDepth() < queryRoot.getDepth()) {
            rootmostMissingAncestor = queryRoot;
        }
        UserTable ancestor = expectedChildren.get(row.getRowDefId()).table;
        nullRowStack.clear();
        while (ancestor != rootmostMissingAncestor) {
            assert ancestor != null : row;
            assert ancestor.getParentJoin() != null : row;
            ancestor = ancestor.getParentJoin().getParent();
            nullRowStack.push(nullRow(ancestor));
        }
        while (!nullRowStack.isEmpty()) {
            missingRows.add(nullRowStack.pop());
        }
    }

    // RowDataGenealogist interface

    public Set<Integer> expectedChildren(int tableId)
    {
        ExpectedChildren expectedChildren = this.expectedChildren.get(tableId);
        assert expectedChildren != null : tableId;
        return expectedChildren.queryChildren;
    }

    public RowDataGenealogist(UserTable queryRoot, Set<UserTable> projectedTables)
    {
        this.queryRoot = queryRoot;
        computeExpectedChildren(queryRoot, projectedTables);
        computeHKeyBoundaries(queryRoot.getAIS());
    }

    // For use by this class

    private RowData nullRow(UserTable table)
    {
        RowData nullRow = nullRows.get(table.getTableId());
        if (nullRow == null) {
            int nColumns = table.getColumnsIncludingInternal().size();
            RowDef rowDef = table.rowDef();
            nullRow = new RowData(new byte[RowData.nullRowBufferSize(rowDef)]);
            nullRow.createRow(rowDef, new Object[nColumns]);
            nullRows.put(table.getTableId(), nullRow);
        }
        return nullRow;
    }

    private void computeExpectedChildren(UserTable queryRoot, Set<UserTable> projectedTables)
    {
        // Sort by depth so that expectedChildren can be computed in one pass
        List<UserTable> tables = new ArrayList<UserTable>(projectedTables);
        Collections.sort(tables,
                         new Comparator<UserTable>()
                         {
                             @Override
                             public int compare(UserTable x, UserTable y)
                             {
                                 return x.getDepth() - y.getDepth();
                             }
                         });
        // For each table in tables, add table to expectedChildren of parent.
        for (UserTable table : tables) {
            ExpectedChildren replaced = expectedChildren.put(table.getTableId(), new ExpectedChildren(table));
            assert replaced == null : table;
            Join parentJoin = table.getParentJoin();
            if (parentJoin != null) {
                UserTable parent = parentJoin.getParent();
                ExpectedChildren parentInfo = expectedChildren.get(parent.getTableId());
                if (parentInfo != null) {
                    parentInfo.queryChildren.add(table.getTableId());
                }
            }
        }
        // Set expected children of the entire query.
        UserTable queryRootParent = queryRoot.getParentJoin() == null ? null : queryRoot.getParentJoin().getParent();
        int queryRootParentId = queryRootParent == null ? JsonOutputter.ROOT_PARENT : queryRootParent.getTableId();
        ExpectedChildren queryRootInfo = new ExpectedChildren(queryRootParent);
        queryRootInfo.queryChildren.add(queryRoot.getTableId());
        expectedChildren.put(queryRootParentId, queryRootInfo);
    }

    private void computeHKeyBoundaries(AkibanInformationSchema ais)
    {
        for (UserTable userTable : ais.getUserTables().values()) {
            HKey hKey = userTable.hKey();
            List<UserTable> boundaries = new ArrayList<UserTable>();
            for (HKeySegment hKeySegment : hKey.segments()) {
                boundaries.add(hKeySegment.table());
                for (int c = 0; c < hKeySegment.columns().size(); c++) {
                    boundaries.add(hKeySegment.table());
                }
            }
            UserTable[] boundariesArray = new UserTable[boundaries.size()];
            boundaries.toArray(boundariesArray);
            hKeyBoundaries.put(userTable.getTableId(), boundariesArray);
        }
    }

    // Object state

    private final UserTable queryRoot;
    private final Map<Integer, ExpectedChildren> expectedChildren = new HashMap<Integer, ExpectedChildren>();
    private final Map<Integer, RowData> nullRows = new HashMap<Integer, RowData>();
    private final Map<Integer, UserTable[]> hKeyBoundaries = new HashMap<Integer, UserTable[]>();
    private final Stack<RowData> nullRowStack = new Stack<RowData>();

    private static class ExpectedChildren
    {
        @Override
        public String toString()
        {
            return table == null ? "null" : table.toString();
        }

        ExpectedChildren(UserTable table)
        {
            this.table = table;
        }

        UserTable table;
        Set<Integer> queryChildren = new HashSet<Integer>();
    }
}
