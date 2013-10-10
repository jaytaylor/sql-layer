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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public final class ImmutableRowTest {
    @Test
    public void basic() {
        Value vh1 = new Value(MNumeric.BIGINT.instance(false), 1L);
        Value vh2 = new Value(MString.varchar(), "right");
        Row row = new ImmutableRow (rowType(MNumeric.BIGINT.instance(false), MString.varchar()), Arrays.asList(vh1, vh2).iterator());
        vh1.putInt64(50L);
        vh2.putString("wrong", null);
        assertEquals("1", 1L, row.value(0).getInt64());
        assertEquals("2", "right", row.value(1).getString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooFewInputs() {
        new ImmutableRow(rowType(MNumeric.INT.instance(false)), Collections.<ValueSource>emptyList().iterator());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooManyInputs() {
        new ImmutableRow(
                rowType(MNumeric.BIGINT.instance(false)),
                Arrays.asList(
                        new Value(MNumeric.BIGINT.instance(false), 1L),
                        new Value(MString.varchar(), "bonus")).iterator()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void wrongInputType() {
        new ImmutableRow(
                rowType(MNumeric.INT.instance(false)),
                Collections.singleton(new Value(MNumeric.INT.instance(false), "1L")).iterator()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void tryToClear() {
        ImmutableRow row = new ImmutableRow(
                rowType(MNumeric.INT.instance(false)),
                Collections.singleton(new Value(MString.varchar(), "1L")).iterator()
        );
        row.clear();
    }

    
    @Test(expected = IllegalStateException.class)
    public void tryToGetHolder() {
        ImmutableRow row = new ImmutableRow(
                rowType(MString.varchar()),
                Collections.singleton(new Value(MString.varchar(), "1L")).iterator()
        );
        row.valueAt(0);
    }

    private RowType rowType(TInstance... types) {
        return new ValuesRowType (null, 1, types);
    }
}
