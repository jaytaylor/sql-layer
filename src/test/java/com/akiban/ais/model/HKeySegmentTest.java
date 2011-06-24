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

package com.akiban.ais.model;

import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class HKeySegmentTest {
    
    @Test
    public void nonCascading() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA)
                .userTable("c")
                    .colLong("cid")
                    .colString("name", 64)
                    .pk("cid")
                .userTable("o")
                    .colLong("oid")
                    .colLong("c_id")
                    .colString("date", 32)
                    .pk("oid")
                    .joinTo("c").on("c_id", "cid")
                .userTable("i")
                    .colLong("iid")
                    .colLong("o_id")
                    .colLong("sku")
                    .pk("iid")
                    .joinTo("o").on("o_id", "oid")
                .ais();

        checkHKeyColumn(ais, "c", 1, 0, 1, 0, 0, c("c", "cid"), c("o", "c_id"));

        checkHKeyColumn(ais, "o", 2, 0, 1, 0, 1, c("c", "cid"), c("o", "c_id"));
        checkHKeyColumn(ais, "o", 2, 1, 1, 0, 0, c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(ais, "i", 3, 0, 1, 0, 1, c("c", "cid"), c("o", "c_id"));
        checkHKeyColumn(ais, "i", 3, 1, 1, 0, 1, c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(ais, "i", 3, 2, 1, 0, 0, c("i", "iid"));
    }

    private void checkHKeyColumn(AkibanInformationSchema ais, String tableName,
                                 int expectedSegments, int checkSegment,
                                 int expectedColumns, int checkColumn,
                                 int mainColumnIndex,
                                 ColumnName... equivalentColumns)
    {
        List<HKeySegment> segments = ais.getUserTable(SCHEMA, tableName).hKey().segments();
        assertEquals("segments size for " + segments, expectedSegments, segments.size());
        List<HKeyColumn> hKeyColumns = segments.get(checkSegment).columns();
        assertEquals("hKeyColumns size", expectedColumns, hKeyColumns.size());
        HKeyColumn hKeyColumn = hKeyColumns.get(checkColumn);
        checkColumnName(hKeyColumn.column(),equivalentColumns[mainColumnIndex]);
        checkEquivalentColumns(Arrays.asList(equivalentColumns), hKeyColumn.equivalentColumns());
    }

    private void checkEquivalentColumns(List<ColumnName> expected, List<Column> actual) {
        List<ColumnName> actualNames = new ArrayList<ColumnName>();
        for (Column column : actual) {
            actualNames.add(new ColumnName(column));
        }
        assertEquals("equivalent columns", expected, actualNames);
    }

    private static void checkColumnName(Column column, ColumnName expected) {
        ColumnName actual = new ColumnName(column);
        assertEquals("column name", expected,  actual);
    }

    private static ColumnName c(String table, String column) {
        return new ColumnName(SCHEMA, table, column);
    }

    // consts
    private static final String SCHEMA = "hkeytest";

    private static class ColumnName {

        public ColumnName(String schema, String table, String column) {
            this.tableName = new TableName(schema, table);
            this.columnName = column;
        }

        public ColumnName(Column column) {
            this.tableName = column.getTable().getName();
            this.columnName = column.getName();
        }

        @Override
        public String toString() {
            return String.format("%s.%s.%s", tableName.getSchemaName(), tableName.getTableName(), columnName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnName that = (ColumnName) o;

            if (!columnName.equals(that.columnName)) return false;
            if (!tableName.equals(that.tableName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tableName.hashCode();
            result = 31 * result + columnName.hashCode();
            return result;
        }

        private final TableName tableName;
        private final String columnName;
    }
}
