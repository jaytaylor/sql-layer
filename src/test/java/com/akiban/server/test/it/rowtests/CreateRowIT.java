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

package com.akiban.server.test.it.rowtests;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import static junit.framework.Assert.fail;
import static junit.framework.Assert.assertEquals;

public class CreateRowIT extends ITBase
{
    @Test
    public void smallRowCantGrow() throws InvalidOperationException
    {
        int t = createTable("s", "t",
                            "string varchar(100) character set latin1");
        RowDef rowDef = rowDefCache().getRowDef(t);
        RowData rowData = new RowData(new byte[RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5]);
        rowData.createRow(rowDef, new Object[]{"abc"}, false);
    }

    @Test(expected=EncodingException.class)
    public void bigRowCantGrow() throws InvalidOperationException
    {
        int t = createTable("s", "t",
                            "string varchar(100)");
        RowDef rowDef = rowDefCache().getRowDef(t);
        RowData rowData = new RowData(new byte[RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5]);
        rowData.createRow(rowDef, new Object[]{"abcdefghijklmnopqrstuvwxyz"}, false);
        fail();
    }

    @Test
    public void growALittle() throws InvalidOperationException
    {
        // Buffer should grow one time
        int t = createTable("s", "t",
                            "string varchar(100)");
        RowDef rowDef = rowDefCache().getRowDef(t);
        int initialBufferSize = RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5;
        RowData rowData = new RowData(new byte[initialBufferSize]);
        rowData.createRow(rowDef, new Object[]{"abcdefghijklmno"}, true);
        assertEquals(initialBufferSize * 2, rowData.getBytes().length);
    }

    @Test
    public void growALot() throws InvalidOperationException
    {
        // Buffer should grow two times
        int t = createTable("s", "t",
                            "string varchar(100)");
        RowDef rowDef = rowDefCache().getRowDef(t);
        int initialBufferSize = RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5;
        RowData rowData = new RowData(new byte[initialBufferSize]);
        // initialBufferSize has room for varchar of size 4, (1 byte of the 5 is for length).
        // initialBufferSize is 24:
        assertEquals(24, initialBufferSize);
        // Doubling it leaves room for 28 chars. Try something a little bigger than that.
        rowData.createRow(rowDef, new Object[]{"abcdefghijklmnopqrstuvwxyz0123"}, true);
        assertEquals(initialBufferSize * 4, rowData.getBytes().length);
    }
}
