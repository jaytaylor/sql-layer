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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;

import java.util.*;

public class RowDataGenealogist implements Genealogist<RowData>
{
    // Genealogist interface

    @Override
    public void fillInMissing(RowData x, RowData y, Queue<RowData> missingRows)
    {
        // x and y are two consecutive rows in a query result. (For the first row of a query result, x is null
        // and y is the first row.)
        // - If x is an ancestor of y: generate null rows for each type between x and y exclusive.
        // - If x is not an ancestor of y: generate null rows for each type above y.
        // For purposes of this implementation, x is an ancestor of y is type(x) is an ancestor of type(y),
        // and if the key fields match according to ancestorJoin(type(x), type(y)). It would be better to
        // compare hkeys, but we don't have access to those here, For a rationale of this approach, see
        // http://akibainc.onconfluence.com/display/db/Determining+whether+a+row+is+an+orphan.
        UserTable xTable = x == null ? queryRootParent : queryTables.get(x.getRowDefId()).table;
        UserTable yTable = queryTables.get(y.getRowDefId()).table;
        if ((xTable == null ? 0 : xTable.getDepth()) < yTable.getDepth()) {
            // x and y are at least one level apart, so y could be an orphan. Build a stack of ancestors of y,
            // so that the stack top is rootmost. We don't yet know that x is an ancestor of y, but this stack will
            // allow us to check, and give us a usefully ordered set of ancestors if it is.
            UserTable yAncestor = yTable;
            Stack<UserTable> ancestors = new Stack<UserTable>();
            while (yAncestor != null && yAncestor != xTable) {
                yAncestor = yAncestor.getParentJoin() == null ? null : yAncestor.getParentJoin().getParent();
                if (yAncestor != xTable) {
                    ancestors.push(yAncestor);
                }
            }
            if (yAncestor == xTable) {
/*
                // x's type is an ancestor of y's type.
                // Now check that PK/FK field values match.
                if (xTable != null && x != null) {
                    RowDef xRowDef = xTable.rowDef();
                    RowDef yRowDef = yTable.rowDef();
                    AncestorJoin ancestorJoin = ancestorJoin(yTable, xTable);
                    int[] ancestorColumnPositions = ancestorJoin.ancestorColumnPositions();
                    int[] descendentColumnPositions = ancestorJoin.descendentColumnPositions();
                    for (int i = 0; ancestor && i < ancestorColumnPositions.length; i++) {
                        Object ancestorField = x.toObject(xRowDef, ancestorColumnPositions[i]);
                        Object descendentField = y.toObject(yRowDef, descendentColumnPositions[i]);
                        ancestor = ancestorField.equals(descendentField);
                    }
                }
                // Fill in missing rows, preorder (ancestors before descendents).
*/
                while (!ancestors.isEmpty()) {
                    missingRows.add(nullRow(ancestors.pop()));
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
            nullRow.createRow((RowDef) table.rowDef(), new Object[table.getColumnsIncludingInternal().size()]);
            nullRows.put(table.getTableId(), nullRow);
        }
        return nullRow;
    }

    private AncestorJoin ancestorJoin(UserTable descendent, UserTable ancestor)
    {
        long key = ancestor.getTableId().longValue() << 32 | descendent.getTableId();
        AncestorJoin ancestorJoin = ancestorJoins.get(key);
        if (ancestorJoin == null) {
            ancestorJoin = new AncestorJoin(descendent);
            computeAncestorJoin(descendent, ancestor, ancestorJoin);
            ancestorJoins.put(key, ancestorJoin);
        }
        return ancestorJoin;
    }

    private void computeAncestorJoin(UserTable descendent, UserTable ancestor, AncestorJoin ancestorJoin)
    {
        // The AncestorJoin objects computed here are a lot like the RowDef arrays used to optimize RowData access.
        // One difference is that an AncestorJoin pertains to a pair of types, not a single type. However, it might
        // be a good idea to compute AncestorJoins once per schema.
        UserTable descendentParent = descendent.getParentJoin().getParent();
        if (descendentParent != ancestor) {
            Join joinToGrandParent = descendentParent.getParentJoin();
            // For each column in ancestorColumns, remove that column and its counterpart in descendentColumns if the
            // ancestorColumn is not a child column in joinToGrandParent. Otherwise, replace the column
            // by its counterpart in the grandparent.
            List<Column> newAncestorColumns = new ArrayList<Column>();
            List<Column> joinToGrandParentChildColumns = new ArrayList<Column>();
            List<Column> joinToGrandParentParentColumns = new ArrayList<Column>();
            for (JoinColumn joinColumn : joinToGrandParent.getJoinColumns()) {
                joinToGrandParentChildColumns.add(joinColumn.getChild());
                joinToGrandParentParentColumns.add(joinColumn.getParent());
            }
            Iterator<Column> ancestorColumnsIterator = ancestorJoin.ancestorColumns.iterator();
            Iterator<Column> descendentColumnsIterator = ancestorJoin.descendentColumns.iterator();
            while (ancestorColumnsIterator.hasNext()) {
                Column ancestorColumn = ancestorColumnsIterator.next();
                descendentColumnsIterator.next();
                int p = joinToGrandParentChildColumns.indexOf(ancestorColumn);
                if (p >= 0) {
                    newAncestorColumns.add(joinToGrandParentParentColumns.get(p));
                } else {
                    ancestorColumnsIterator.remove();
                    descendentColumnsIterator.remove();
                }
            }
            ancestorJoin.ancestorColumns = newAncestorColumns;
            // Recurse, getting one step closer to ancestor
            computeAncestorJoin(descendentParent, ancestor, ancestorJoin);
        }
    }

    // Object state

    private UserTable queryRootParent;
    private Map<Integer, TableInfo> queryTables;
     // key of ancestorJoins: descendent table id in low bits, ancestor table id in high bits.
    private Map<Long, AncestorJoin> ancestorJoins = new HashMap<Long, AncestorJoin>();
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

    private static class AncestorJoin
    {
        int[] ancestorColumnPositions()
        {
            if (ancestorColumnPositions == null) {
                gatherColumnPositions();
            }
            return ancestorColumnPositions;
        }

        int[] descendentColumnPositions()
        {
            if (descendentColumnPositions == null) {
                gatherColumnPositions();
            }
            return descendentColumnPositions;
        }

        AncestorJoin(UserTable descendentTable)
        {
            descendentColumns = new ArrayList<Column>();
            ancestorColumns = new ArrayList<Column>();
            for (JoinColumn joinColumn : descendentTable.getParentJoin().getJoinColumns()) {
                descendentColumns.add(joinColumn.getChild());
                ancestorColumns.add(joinColumn.getParent());
            }
        }

        private void gatherColumnPositions()
        {
            assert ancestorColumns.size() == descendentColumns.size();
            ancestorColumnPositions = new int[ancestorColumns.size()];
            descendentColumnPositions = new int[descendentColumns.size()];
            for (int i = 0; i < ancestorColumns.size(); i++) {
                ancestorColumnPositions[i] = ancestorColumns.get(i).getPosition();
                descendentColumnPositions[i] = descendentColumns.get(i).getPosition();
            }
            descendentColumns = null;
            ancestorColumns = null;
        }

        private List<Column> descendentColumns;
        private List<Column> ancestorColumns;
        private int[] descendentColumnPositions;
        private int[] ancestorColumnPositions;
    }
}
