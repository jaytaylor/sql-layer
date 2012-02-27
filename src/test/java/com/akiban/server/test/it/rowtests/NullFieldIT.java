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

package com.akiban.server.test.it.rowtests;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

public class NullFieldIT extends OldTypeITBase
{
    @Before
    public final void turnOnIndexes() {
        setCreateIndexes(true);
    }

    @Test
    public void intEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("int");
        writeRows(createNewRow(tid, 1, 10), createNewRow(tid, 2, null));
    }

    @Test
    public void uintEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("int unsigned");
        writeRows(createNewRow(tid, 1, 10), createNewRow(tid, 2, null));
    }
    
    @Test
    public void ubigintEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("bigint unsigned");
        writeRows(createNewRow(tid, 1, BigInteger.valueOf(10)), createNewRow(tid, 2, null));
    }

    @Test
    public void floatEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("float");
        writeRows(createNewRow(tid, 1, 1.142), createNewRow(tid, 2, null));
    }

    @Test
    public void ufloatEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("float unsigned");
        writeRows(createNewRow(tid, 1, 1.42), createNewRow(tid, 2, null));
    }

    @Test
    public void decimalEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes(new TypeAndParams("decimal", 10L, 2L));
        writeRows(createNewRow(tid, 1, BigDecimal.valueOf(110, 2)), createNewRow(tid, 2, null));
    }

    @Test
    public void doubleEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("double");
        writeRows(createNewRow(tid, 1, 1.142), createNewRow(tid, 2, null));
    }

    @Test
    public void udoubleEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("double unsigned");
        writeRows(createNewRow(tid, 1, 1.42), createNewRow(tid, 2, null));
    }

    @Test
    public void stringEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes(new TypeAndParams("varchar", 32L, null));
        writeRows(createNewRow(tid, 1, "hello"), createNewRow(tid, 2, null));
    }

    @Test
    public void varbinaryEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes(new TypeAndParams("varbinary", 32L, null));
        writeRows(createNewRow(tid, 1, new byte[]{0x71,0x65}), createNewRow(tid, 2, null));
    }

    @Test
    public void dateEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("date");
        writeRows(createNewRow(tid, 1, "2011-04-20"), createNewRow(tid, 2, null));
    }

    @Test
    public void timeEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("time");
        writeRows(createNewRow(tid, 1, "14:10:00"), createNewRow(tid, 2, null));
    }

    @Test
    public void datetimeEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("datetime");
        writeRows(createNewRow(tid, 1, "2011-04-20 14:11:00"), createNewRow(tid, 2, null));
    }

    @Test
    public void timestampEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("timestamp");
        writeRows(createNewRow(tid, 1, "2011-04-20 14:11:47"), createNewRow(tid, 2, null));
    }

    @Test
    public void yearEncoder() throws InvalidOperationException {
        final int tid = createTableFromTypes("year");
        writeRows(createNewRow(tid, 1, "2011"), createNewRow(tid, 2, null));
    }
}
