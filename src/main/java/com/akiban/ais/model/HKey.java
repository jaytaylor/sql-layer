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

package com.akiban.ais.model;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.pvalue.PUnderlying;

import java.util.ArrayList;
import java.util.List;

public class HKey
{
    @Override
    public synchronized String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("HKey(");
        boolean firstTable = true;
        for (HKeySegment segment : segments) {
            if (firstTable) {
                firstTable = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(segment.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }
    
    public UserTable userTable()
    {
        return (UserTable) table;
    }

    public List<HKeySegment> segments()
    {
        return segments;
    }

    public int nColumns()
    {
        ensureDerived();
        return columns.length;
    }
    
    public Column column(int i)
    {
        ensureDerived();
        return columns[i];
    }
    
    public AkType columnType(int i)
    {
        ensureDerived();
        return columnTypes[i];
    }

    public HKey(Table table)
    {
        this.table = table;
    }

    public synchronized HKeySegment addSegment(UserTable segmentTable)
    {
        assert keyDepth == null : segmentTable; // Should only be computed after HKeySegments are completely formed.
        UserTable lastSegmentTable = segments.isEmpty() ? null : segments.get(segments.size() - 1).table();
        assert segmentTable.parentTable() == lastSegmentTable;
        HKeySegment segment = new HKeySegment(this, segmentTable);
        segments.add(segment);
        return segment;
    }

    public boolean containsColumn(Column column) 
    {
        ensureDerived();
        for (Column c : columns) {
            if (c == column) {
                return true;
            }
        }
        return false;
    }

    public int[] keyDepth()
    {
        ensureDerived();
        return keyDepth;
    }

    // For use by this class

    private void ensureDerived()
    {
        if (columns == null) {
            synchronized (this) {
                if (columns == null) {
                    // columns
                    List<Column> columnList = new ArrayList<Column>();
                    for (HKeySegment segment : segments) {
                        for (HKeyColumn hKeyColumn : segment.columns()) {
                            columnList.add(hKeyColumn.column());
                        }
                    }
                    columns = new Column[columnList.size()];
                    columnTypes = new AkType[columnList.size()];
                    int c = 0;
                    for (Column column : columnList) {
                        Type type = column.getType();
                        columns[c] = column;
                        columnTypes[c] = type.akType();
                        c++;
                    }
                    // keyDepth
                    keyDepth = new int[segments.size() + 1];
                    int hKeySegments = segments.size();
                    for (int hKeySegment = 0; hKeySegment < hKeySegments; hKeySegment++) {
                        this.keyDepth[hKeySegment] =
                            hKeySegment == 0
                            ? 1
                            // + 1 to account for the ordinal
                            : keyDepth[hKeySegment - 1] + 1 + segments.get(hKeySegment - 1).columns().size();
                    }
                    keyDepth[hKeySegments] = columns.length + hKeySegments;
                }
            }
        }
    }

    // State

    private final Table table;
    private final List<HKeySegment> segments = new ArrayList<HKeySegment>();
    // keyDepth[n] is the number of key segments (ordinals + key values) comprising an hkey of n parts.
    // E.g. keyDepth[1] for the hkey of the root segment.
    private int[] keyDepth;
    private Column[] columns;
    private AkType[] columnTypes;
}
