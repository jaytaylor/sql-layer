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
