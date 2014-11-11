/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.rowtests;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.rowdata.encoding.EncodingException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class CreateRowIT extends ITBase
{
    @Test
    public void smallRowCantGrow() throws InvalidOperationException
    {
        int t = createTable("s", "t",
                            "string varchar(100) character set latin1");
        RowDef rowDef = getRowDef(t);
        RowData rowData = new RowData(new byte[RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5]);
        rowData.createRow(rowDef, new Object[]{"abc"}, false);
    }

    @Test(expected=EncodingException.class)
    public void bigRowCantGrow() throws InvalidOperationException
    {
        int t = createTable("s", "t",
                            "string varchar(100)");
        RowDef rowDef = getRowDef(t);
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
        RowDef rowDef = getRowDef(t);
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
        RowDef rowDef = getRowDef(t);
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
