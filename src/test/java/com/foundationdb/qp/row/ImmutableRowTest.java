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
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public final class ImmutableRowTest {
    @Test
    public void basic() {
        ValueHolder vh1 = new ValueHolder(AkType.LONG, 1L);
        ValueHolder vh2 = new ValueHolder(AkType.VARCHAR, "right");
        Row row = new ImmutableRow(rowType(AkType.LONG, AkType.VARCHAR), Arrays.asList(vh1, vh2).iterator());
        vh1.putLong(50);
        vh2.putString("wrong");
        assertEquals("1", 1L, row.eval(0).getLong());
        assertEquals("2", "right", row.eval(1).getString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooFewInputs() {
        new ImmutableRow(rowType(AkType.LONG), Collections.<ValueSource>emptyList().iterator());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooManyInputs() {
        new ImmutableRow(
                rowType(AkType.LONG),
                Arrays.asList(
                        new ValueHolder(AkType.LONG, 1L),
                        new ValueHolder(AkType.VARCHAR, "bonus")).iterator()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongInputType() {
        new ImmutableRow(
                rowType(AkType.LONG),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void tryToClear() {
        ImmutableRow row = new ImmutableRow(
                rowType(AkType.VARCHAR),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
        row.clear();
    }

    @Test(expected = IllegalStateException.class)
    public void tryToGetHolder() {
        ImmutableRow row = new ImmutableRow(
                rowType(AkType.VARCHAR),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
        row.holderAt(0);
    }

    @Test
    public void aquire() {
        Row row = new ImmutableRow(
                rowType(AkType.VARCHAR),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
        row.acquire();
        row.acquire();
        row.acquire();
        assertEquals("isShared", false, row.isShared());
    }

    private RowType rowType(AkType... types) {
        return new ValuesRowType(null, 1, types);
    }
}
