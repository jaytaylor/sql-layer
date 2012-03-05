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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
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
