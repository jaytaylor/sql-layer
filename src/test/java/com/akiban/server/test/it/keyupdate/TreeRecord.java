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

package com.akiban.server.test.it.keyupdate;

import com.akiban.server.api.dml.scan.NewRow;

public class TreeRecord
{
    @Override
    public int hashCode()
    {
        return hKey.hashCode() ^ row.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        boolean eq = o != null && o instanceof TreeRecord;
        if (eq) {
            TreeRecord that = (TreeRecord) o;
            eq = this.hKey.equals(that.hKey) && equals(this.row, that.row);
        }
        return eq;
    }

    @Override
    public String toString()
    {
        return String.format("%s -> %s", hKey, row);
    }

    public HKey hKey()
    {
        return hKey;
    }

    public NewRow row()
    {
        return row;
    }

    public TreeRecord(HKey hKey, NewRow row)
    {
        this.hKey = hKey;
        this.row = row;
    }

    public TreeRecord(Object[] hKey, NewRow row)
    {
        this(new HKey(hKey), row);
    }

    private boolean equals(NewRow x, NewRow y)
    {
        return
            x == y ||
            x != null && y != null && x.getRowDef() == y.getRowDef() && x.getFields().equals(y.getFields());
    }

    private final HKey hKey;
    private final NewRow row;
}
