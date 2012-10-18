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

package com.akiban.server.test.it.dxl;

import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ByteArrayColumnSelector;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

public class RowUpdateIT extends ITBase
{
    @Test
    public void testReplaceEntireRow() throws InvalidOperationException
    {
        initializeDB();
        NewRow targetUpdated = new NiceRow(t, rowDef);
        targetUpdated.put(cId, 888L);
        targetUpdated.put(cA, 800L);
        targetUpdated.put(cB, 800L);
        targetUpdated.put(cC, 800L);
        targetUpdated.put(cD, 800L);
        dml().updateRow(session(), target, targetUpdated, null);
        expectFullRows(t, before, targetUpdated, after);
    }

    @Test
    public void testReplacePartialRow() throws InvalidOperationException
    {
        initializeDB();
        // Column positions:
        //     id: 0
        //     a: 1
        //     b: 2
        //     c: 3
        //     d: 4
        // Generate all combinations of a..d by iterating a mask from 1..31 in steps of 2.
        long original = 888L;
        long update = 800L;
        NiceRow fullyUpdatedTarget = new NiceRow(t, rowDef);
        fullyUpdatedTarget.put(cId, original);
        fullyUpdatedTarget.put(cA, update);
        fullyUpdatedTarget.put(cB, update);
        fullyUpdatedTarget.put(cC, update);
        fullyUpdatedTarget.put(cD, update);
        NiceRow partiallyUpdatedTarget = new NiceRow(t, rowDef);
        partiallyUpdatedTarget.put(cId, original);
        byte mask = 0x01; // just id
        while (mask <= 0x1f) {
            // Generate expected updated row
            partiallyUpdatedTarget.put(cA, (mask & 0x02) == 0 ? original : update);
            partiallyUpdatedTarget.put(cB, (mask & 0x04) == 0 ? original : update);
            partiallyUpdatedTarget.put(cC, (mask & 0x08) == 0 ? original : update);
            partiallyUpdatedTarget.put(cD, (mask & 0x10) == 0 ? original : update);
            ColumnSelector columnSelector = new ByteArrayColumnSelector(new byte[]{mask});
            dml().updateRow(session(), target, fullyUpdatedTarget, columnSelector);
            expectFullRows(t, before, partiallyUpdatedTarget, after);
            dml().updateRow(session(), partiallyUpdatedTarget, target, null);
            expectFullRows(t, before, target, after);
            mask += 2;
        }
        expectFullRows(t, before, target, after);
    }

    private void initializeDB() throws InvalidOperationException
    {
        t = createTable("s", "t",
                        "id int not null primary key",
                        "a int",
                        "b int",
                        "c int",
                        "d int");
        cId = 0;
        cA = 1;
        cB = 2;
        cC = 3;
        cD = 4;
        rowDef = getRowDef(t);
        target = new NiceRow(t, rowDef);
        target.put(cId, 888L);
        target.put(cA, 888L);
        target.put(cB, 888L);
        target.put(cC, 888L);
        target.put(cD, 888L);
        dml().writeRow(session(), target);
        before = new NiceRow(t, rowDef);
        before.put(cId, 777L);
        before.put(cA, 777L);
        before.put(cB, 777L);
        before.put(cC, 777L);
        before.put(cD, 777L);
        dml().writeRow(session(), before);
        after = new NiceRow(t, rowDef);
        after.put(cId, 999L);
        after.put(cA, 999L);
        after.put(cB, 999L);
        after.put(cC, 999L);
        after.put(cD, 999L);
        dml().writeRow(session(), after);
        expectFullRows(t, before, target, after);
    }

    private int t;
    private int cId;
    private int cA;
    private int cB;
    private int cC;
    private int cD;
    private RowDef rowDef;
    private NewRow before;
    private NewRow target;
    private NewRow after;
}
