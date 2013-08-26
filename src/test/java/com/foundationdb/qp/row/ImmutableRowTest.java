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

package com.foundationdb.qp.row;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.ValuesRowType;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public final class ImmutableRowTest {
    @Test
    public void basic() {
        PValue vh1 = new PValue (MNumeric.BIGINT.instance(false), 1L);
        PValue vh2 = new PValue (MString.varchar(), "right");
        Row row = new ImmutableRow (rowType(MNumeric.BIGINT.instance(false), MString.varchar()), Arrays.asList(vh1, vh2).iterator());
        vh1.putInt64(50L);
        vh2.putString("wrong", null);
        assertEquals("1", 1L, row.pvalue(0).getInt64());
        assertEquals("2", "right", row.pvalue(1).getString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooFewInputs() {
        new ImmutableRow(rowType(MNumeric.INT.instance(false)), Collections.<PValueSource>emptyList().iterator());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooManyInputs() {
        new ImmutableRow(
                rowType(MNumeric.BIGINT.instance(false)),
                Arrays.asList(
                        new PValue(MNumeric.BIGINT.instance(false), 1L),
                        new PValue(MString.varchar(), "bonus")).iterator()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void wrongInputType() {
        new ImmutableRow(
                rowType(MNumeric.INT.instance(false)),
                Collections.singleton(new PValue(MNumeric.INT.instance(false), "1L")).iterator()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void tryToClear() {
        ImmutableRow row = new ImmutableRow(
                rowType(MNumeric.INT.instance(false)),
                Collections.singleton(new PValue(MString.varchar(), "1L")).iterator()
        );
        row.clear();
    }

    
    @Test(expected = IllegalStateException.class)
    public void tryToGetHolder() {
        ImmutableRow row = new ImmutableRow(
                rowType(MString.varchar()),
                Collections.singleton(new PValue(MString.varchar(), "1L")).iterator()
        );
        row.pvalueAt(0);
    }

    @Test
    public void aquire() {
        Row row = new ImmutableRow(
                rowType(MString.varchar()),
                Collections.singleton(new PValue(MString.varchar(), "1L")).iterator()
        );
        row.acquire();
        row.acquire();
        row.acquire();
        assertEquals("isShared", false, row.isShared());
    }

    
    private RowType rowType(TInstance... types) {
        return new ValuesRowType (null, 1, types);
    }
}
