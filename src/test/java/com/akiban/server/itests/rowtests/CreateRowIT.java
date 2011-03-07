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

package com.akiban.server.itests.rowtests;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.encoding.EncodingException;
import com.akiban.server.itests.ApiTestBase;
import org.junit.Test;

import static junit.framework.Assert.fail;
import static junit.framework.Assert.assertEquals;

public class CreateRowIT extends ApiTestBase
{
    @Test
    public void smallRowCantGrow() throws InvalidOperationException
    {
        int t = createTable("s", "t",
                            "string varchar(100)");
        RowDef rowDef = rowDefCache().getRowDef(t);
        RowData rowData = new RowData(new byte[RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5]);
        try {
            rowData.createRow(rowDef, new Object[]{"abc"}, false);
        } catch (EncodingException e) {
            fail();
        }
    }

    @Test
    public void bigRowCantGrow() throws InvalidOperationException
    {
        int t = createTable("s", "t",
                            "string varchar(100)");
        RowDef rowDef = rowDefCache().getRowDef(t);
        RowData rowData = new RowData(new byte[RowData.MINIMUM_RECORD_LENGTH + 1 /* null bitmap */ + 5]);
        try {
            rowData.createRow(rowDef, new Object[]{"abcdefghijklmnopqrstuvwxyz"}, false);
            fail();
        } catch (EncodingException e) {
        }
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
        // Buffer should grow one time
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
