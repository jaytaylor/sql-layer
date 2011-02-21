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

import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowData;

import java.util.*;

public class RowDataGenealogist implements Genealogist<RowData>
{
    // Genealogist interface

    @Override
    public void fillInDescendents(RowData x, RowData y, Queue<RowData> missing)
    {
        UserTable xTable = x == null ? queryRootParent : queryTables.get(x.getRowDefId()).table;
        UserTable yTable = queryTables.get(y.getRowDefId()).table;
        if ((xTable == null ? 0 : xTable.getDepth() + 1) < yTable.getDepth()) {
            // x and y are at least two levels apart, so y could be an orphan. Now check to see if
            // x is an ancestor of y.
            UserTable yAncestor = yTable;
            Stack<UserTable> needNullRows = new Stack<UserTable>();
            while (yAncestor != null && yAncestor != xTable) {
                yAncestor = yAncestor.getParentJoin() == null ? null : yAncestor.getParentJoin().getParent();
                if (yAncestor != xTable) {
                    needNullRows.push(yAncestor);
                }
            }
            if (yAncestor == xTable) {
                // x is an ancestor of y. Fill in missing rows, preorder (ancestors before descendents).
                while (!needNullRows.isEmpty()) {
                    missing.add(nullRow(needNullRows.pop()));
                }
            }
        }
    }

    // RowDataGenealogist interface

    public Set<Integer> expectedChildren(int tableId)
    {
        TableInfo tableInfo = queryTables.get(tableId);
        assert tableInfo != null : tableId;
        return tableInfo.queryChildren;
    }

    public RowDataGenealogist(UserTable queryRoot, Set<UserTable> projectedTables)
    {
        // Sort by depth so that queryTables can be computed in one pass
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
        // For each table in tables, add table to queryTables of parent.
        queryTables = new HashMap<Integer, TableInfo>();
        for (UserTable table : tables) {
            TableInfo replaced = queryTables.put(table.getTableId(), new TableInfo(table));
            assert replaced == null : table;
            Join parentJoin = table.getParentJoin();
            if (parentJoin != null) {
                UserTable parent = parentJoin.getParent();
                TableInfo parentInfo = queryTables.get(parent.getTableId());
                if (parentInfo != null) {
                    parentInfo.queryChildren.add(table.getTableId());
                }
            }
        }
        // Set expected children of the entire query.
        queryRootParent = queryRoot.getParentJoin() == null ? null : queryRoot.getParentJoin().getParent();
        int queryRootParentId = queryRootParent == null ? JsonOutputter.ROOT_PARENT : queryRootParent.getTableId();
        TableInfo queryRootInfo = new TableInfo(queryRootParent);
        queryRootInfo.queryChildren.add(queryRoot.getTableId());
        queryTables.put(queryRootParentId, queryRootInfo);
    }

    // For use by this class

    private RowData nullRow(UserTable table)
    {
        RowData nullRow = nullRows.get(table.getTableId());
        if (nullRow == null) {
            nullRow = new RowData(new byte[1000]); // TODO: 1000 is a guess
            nullRow.createRow(table.rowDef(), new Object[table.getColumnsIncludingInternal().size()]);
            nullRows.put(table.getTableId(), nullRow);
        }
        return nullRow;
    }

    // Object state

    private UserTable queryRootParent;
    private Map<Integer, TableInfo> queryTables;
    private Map<Integer, RowData> nullRows = new HashMap<Integer, RowData>();

    private static class TableInfo
    {
        @Override
        public String toString()
        {
            return table == null ? "null" : table.toString();
        }

        TableInfo(UserTable table)
        {
            this.table = table;
        }

        UserTable table;
        Set<Integer> queryChildren = new HashSet<Integer>();
    }
}
