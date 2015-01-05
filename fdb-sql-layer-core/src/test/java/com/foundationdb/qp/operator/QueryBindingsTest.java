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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.*;

public class QueryBindingsTest 
{
    @Test
    public void valueTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        ValueSource value = new Value(MNumeric.INT.instance(false), 123);
        bindings.setValue(1, value);
        assertTrue(TClass.areEqual(value, bindings.getValue(1)));
    }

    @Test(expected=BindingNotSetException.class)
    public void unboundValueTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        ValueSource value = new Value(MNumeric.INT.instance(false), 0);
        bindings.setValue(0, value);
        bindings.getValue(1);
    }

    @Test
    public void rowTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        Deque<Row> rows = new RowsBuilder(MNumeric.INT.instance(false))
            .row(100)
            .row(101)
            .rows();
        for (Row row : rows) {
            bindings.setRow(1, row);
            assertEquals(row, bindings.getRow(1));
        }
    }

    @Test(expected=BindingNotSetException.class)
    public void unboundRowTest() {
        QueryBindings bindings = new SparseArrayQueryBindings();
        bindings.getRow(1);
    }

    @Test
    public void inheritanceTest() {
        QueryBindings parent = new SparseArrayQueryBindings();
        assertEquals(0, parent.getDepth());
        ValueSource value = new Value(MNumeric.INT.instance(false), 1);
        parent.setValue(0, value);
        QueryBindings child = parent.createBindings();
        assertEquals(1, child.getDepth());
        assertTrue(parent.isAncestor(parent));
        assertTrue(child.isAncestor(parent));
        assertFalse(parent.isAncestor(child));
        Deque<Row> rows = new RowsBuilder(MNumeric.INT.instance(false))
            .row(100)
            .row(101)
            .rows();
        for (Row row : rows) {
            child.setRow(1, row);
            assertEquals(row, child.getRow(1));
        }
        assertTrue(TClass.areEqual(value, child.getValue(0)));
        try {
            parent.getRow(1);
            fail();
        }
        catch (BindingNotSetException ex) {
        }
    }

}
