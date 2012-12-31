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

package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

public class HKeyRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return hKey.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i)
    {
        return hKey.eval(i);
    }

    @Override
    public PValueSource pvalue(int i) {
        return hKey.pEval(i);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        // TODO: This does the wrong thing for hkeys derived from group index rows!
        // TODO: See bug 997746.
        HKey ancestorHKey = hKeyCache.hKey(table);
        hKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    // HKeyRow interface

    public HKeyRow(HKeyRowType rowType, HKey hKey, HKeyCache<HKey> hKeyCache)
    {
        this.hKeyCache = hKeyCache;
        this.rowType = rowType;
        this.hKey = hKey;
    }
    
    // Object state

    private final HKeyCache<HKey> hKeyCache;
    private final HKeyRowType rowType;
    private HKey hKey;
}
