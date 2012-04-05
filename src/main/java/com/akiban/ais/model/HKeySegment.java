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

import java.util.ArrayList;
import java.util.List;

public class HKeySegment
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(table().getName().getTableName());
        buffer.append(": (");
        boolean firstColumn = true;
        for (HKeyColumn hKeyColumn : columns()) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(hKeyColumn.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }

    public HKeySegment(HKey hKey, UserTable table)
    {
        this.hKey = hKey;
        this.table = table;
        if (hKey.segments().isEmpty()) {
            this.positionInHKey = 0;
        } else {
            HKeySegment lastSegment = hKey.segments().get(hKey.segments().size() - 1);
            this.positionInHKey =
                lastSegment.columns().isEmpty()
                ? lastSegment.positionInHKey() + 1
                : lastSegment.columns().get(lastSegment.columns().size() - 1).positionInHKey() + 1;
        }
    }
    
    public HKey hKey()
    {
        return hKey;
    }

    public UserTable table()
    {
        return table;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public List<HKeyColumn> columns()
    {
        return columns;
    }

    public HKeyColumn addColumn(Column column)
    {
        assert column != null;
        HKeyColumn hKeyColumn = new HKeyColumn(this, column);
        columns.add(hKeyColumn);
        return hKeyColumn;
    }

    private final HKey hKey;
    private final UserTable table;
    private final List<HKeyColumn> columns = new ArrayList<HKeyColumn>();
    private final int positionInHKey;
}
