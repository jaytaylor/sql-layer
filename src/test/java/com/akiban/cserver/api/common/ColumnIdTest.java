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

package com.akiban.cserver.api.common;

import static org.junit.Assert.assertEquals;

import java.util.TreeMap;

import org.junit.Test;

public final class ColumnIdTest {
    @Test
    public void testEquality() {
        ColumnId columnOne = ColumnId.of(1);
        ColumnId columnTwo = ColumnId.of(1);

        assertEquals("columns", columnOne, columnTwo);
        
        Object value = new Object();
        TreeMap<ColumnId,Object> mapOne = new TreeMap<ColumnId, Object>();
        mapOne.put(columnOne, value);
        TreeMap<ColumnId,Object> mapTwo = new TreeMap<ColumnId, Object>();
        mapTwo.put(columnOne, value);
        assertEquals("maps", mapOne, mapTwo);
    }
}
