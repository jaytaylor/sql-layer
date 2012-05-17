/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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

        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("c", "cid"), c("o", "c_id"));

        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("c", "cid"), c("o", "c_id"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("c", "cid"), c("o", "c_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(2).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("i", "iid"));
    }

    @Test
    public void cascading() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA)
                .userTable("c")
                    .colLong("cid")
                    .colString("name", 64)
                    .pk("cid")
                .userTable("o")
                    .colLong("c_id")
                    .colLong("oid")
                    .colString("date", 32)
                    .pk("c_id", "oid")
                    .joinTo("c").on("c_id", "cid")
                .userTable("i")
                    .colLong("c__id")
                    .colLong("o_id")
                    .colLong("iid")
                    .colLong("sku")
                    .pk("c__id", "o_id", "iid")
                    .joinTo("o").on("c__id","c_id").and("o_id", "oid")
                .ais();

        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("c", "cid"), c("o", "c_id"), c("i", "c__id"));

        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("c", "cid"), c("o", "c_id"), c("i", "c__id"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(1).checkCol(0).expectAtIndex(2),
                c("c", "cid"), c("o", "c_id"), c("i", "c__id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(2).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("i", "iid"));
    }

    @Test
    public void multiColumnPkNoCascade() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA)
                .userTable("c")
                    .colLong("cid1")
                    .colLong("cid2")
                    .colString("name", 64)
                    .pk("cid1", "cid2")
                .userTable("o")
                    .colLong("oid")
                    .colLong("c_id1")
                    .colLong("c_id2")
                    .colString("date", 32)
                    .pk("oid")
                    .joinTo("c").on("c_id1", "cid1").and("c_id2", "cid2")
                .userTable("i")
                    .colLong("iid")
                    .colLong("o_id")
                    .colLong("sku")
                    .pk("iid")
                    .joinTo("o").on("o_id", "oid")
                .ais();

        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(2).checkCol(0).expectAtIndex(0),
                c("c", "cid1"), c("o", "c_id1"));
        checkHKeyColumn(
                ais, "c",
                expectedSegments(1).checkSegment(0).expectedCols(2).checkCol(1).expectAtIndex(0),
                c("c", "cid2"), c("o", "c_id2"));

        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(2).checkCol(0).expectAtIndex(1),
                c("c", "cid1"), c("o", "c_id1"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(0).expectedCols(2).checkCol(1).expectAtIndex(1),
                c("c", "cid2"), c("o", "c_id2"));
        checkHKeyColumn(
                ais, "o",
                expectedSegments(2).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("o", "oid"), c("i", "o_id"));

        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(2).checkCol(0).expectAtIndex(1),
                c("c", "cid1"), c("o", "c_id1"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(0).expectedCols(2).checkCol(1).expectAtIndex(1),
                c("c", "cid2"), c("o", "c_id2"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(1).expectedCols(1).checkCol(0).expectAtIndex(1),
                c("o", "oid"), c("i", "o_id"));
        checkHKeyColumn(
                ais, "i",
                expectedSegments(3).checkSegment(2).expectedCols(1).checkCol(0).expectAtIndex(0),
                c("i", "iid"));
    }

    private void checkHKeyColumn(
                AkibanInformationSchema ais, String tableName,
                                 SegmentCheckParameters segmentParameters,
                                 ColumnName... equivalentColumns)
    {
        List<HKeySegment> segments = ais.getUserTable(SCHEMA, tableName).hKey().segments();
        assertEquals("segments size for " + segments, segmentParameters.expectedSegments, segments.size());
        List<HKeyColumn> hKeyColumns = segments.get(segmentParameters.checkSegment).columns();
        assertEquals("hKeyColumns size", segmentParameters.expectedColumns, hKeyColumns.size());
        HKeyColumn hKeyColumn = hKeyColumns.get(segmentParameters.checkColumn);
        checkColumnName(hKeyColumn.column(), equivalentColumns[segmentParameters.mainColumnIndex]);
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
    
    private static SegmentCheckParameters expectedSegments(int expectedSegments) {
        return new SegmentCheckParameters().expectedSegments(expectedSegments);
    }

    // consts
    private static final String SCHEMA = "hkeytest";

    // nested classes

    private static class SegmentCheckParameters {
        /**
         * How many HKeySegments we expect in the HKey
         * @param expectedSegments expected size of {@link HKey#segments()}
         * @return this object (for chaining)
         */
        SegmentCheckParameters expectedSegments(int expectedSegments) {
            this.expectedSegments = expectedSegments;
            return this;
        }

        /**
         * Which segment to check ( must be between 0 and expectedSegments )
         * @param checkSegment the index of {@link HKey#segments()} to check
         * @return this object (for chaining)
         */
        SegmentCheckParameters checkSegment(int checkSegment) {
            this.checkSegment = checkSegment;
            return this;
        }

        /**
         * How many columns we expect in this segment
         * @param expectedColumns the expected size of {@link HKeySegment#columns}
         * @return this object (for chaining)
         */
        SegmentCheckParameters expectedCols(int expectedColumns) {
            this.expectedColumns = expectedColumns;
            return this;
        }

        /**
         * Which column to check
         * @param checkColumn the index of {@link HKeySegment#columns} to check
         * @return this object (for chaining)
         */
        SegmentCheckParameters checkCol(int checkColumn) {
            this.checkColumn = checkColumn;
            return this;
        }

        /**
         * Where in the equivalent columns list we expect the "main" column to be. In other words, we'll check that
         * {@link HKeyColumn#column()} is the same as the entry of the given index in
         * {@link HKeyColumn#equivalentColumns()}.
         * @param mainColumnIndex the index within {@linkplain HKeyColumn#equivalentColumns()} to expect to find
         * {@link HKeyColumn#column()}
         * @return this object (for chaining)
         */
        SegmentCheckParameters expectAtIndex(int mainColumnIndex) {
            this.mainColumnIndex = mainColumnIndex;
            return this;
        }

        private int expectedSegments = -1;
        private int checkSegment = -1;
        private int expectedColumns = -1;
        private int checkColumn = -1;
        private int mainColumnIndex = -1;
    }

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

            return columnName.equals(that.columnName) && tableName.equals(that.tableName);

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
