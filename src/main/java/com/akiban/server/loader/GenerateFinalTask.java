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

package com.akiban.server.loader;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

public abstract class GenerateFinalTask extends Task
{
    public final int[] hKeyColumnPositions()
    {
        if (hKeyColumnPositions == null) {
            hKeyColumnPositions = new int[hKey().size()];
            int p = 0;
            for (Column hKeyColumn : hKey()) {
                int hKeyColumnPosition = columns().indexOf(hKeyColumn);
                if (hKeyColumnPosition == -1) {
                    throw new BulkLoader.InternalError(hKeyColumn.toString());
                }
                hKeyColumnPositions[p++] = hKeyColumnPosition;
            }
        }
        return hKeyColumnPositions;
    }

    public final int[] columnPositions()
    {
        return columnPositions;
    }

    protected GenerateFinalTask(BulkLoader loader, UserTable table)
    {
        super(loader, table, "$final");
    }

    protected String toString(int[] a)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append('[');
        boolean first = true;
        for (int x : a) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(x);
        }
        buffer.append(']');
        return buffer.toString();
    }

    // Positions of columns from the original table in the $final table.
    protected int[] columnPositions;
    // Positions of hkey columns in the $final table
    private int[] hKeyColumnPositions;
}