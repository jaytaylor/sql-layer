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

package com.foundationdb.ais.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnNameTest
{
    @Test
    public void parseFull() {
        testParse(new ColumnName("a", "b", "c"), null, "a.b.c");
    }

    @Test
    public void parseNoSchema() {
        testParse(new ColumnName("def", "a", "b"), "def", "a.b");
    }

    @Test
    public void parseEmpty() {
        testParse(new ColumnName("def", "", ""), "def", "");
    }

    private static void testParse(ColumnName expected, String defSchema, String str) {
        ColumnName actual = ColumnName.parse(defSchema, str);
        assertEquals("schema", expected.getTableName().getSchemaName(), actual.getTableName().getSchemaName());
        assertEquals("table", expected.getTableName().getTableName(), actual.getTableName().getTableName());
        assertEquals("column", expected.getName(), actual.getName());
    }
}
