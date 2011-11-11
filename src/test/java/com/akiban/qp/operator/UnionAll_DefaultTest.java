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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.DerivedTypesSchema;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class UnionAll_DefaultTest {
    
    @Test
    public void unionTwoNormal() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR, AkType.NULL)
                .row(1L, "one", null)
                .row(2L, "two", null)
                .row(1L, "one", null);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR, AkType.NULL)
                .row(3L, "three", null)
                .row(1L, "one", null)
                .row(2L, "deux", null);
        RowsBuilder expected = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR, AkType.NULL)
                .row(1L, "one", null)
                .row(2L, "two", null)
                .row(1L, "one", null)
                .row(3L, "three", null)
                .row(1L, "one", null)
                .row(2L, "deux", null);
        check(first, second, expected);
    }

    @Test
    public void firstInputEmpty() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        RowsBuilder expected = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        check(first, second, expected);
    }

    @Test
    public void secondInputEmpty() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder expected = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        check(first, second, expected);
    }

    @Test
    public void nullPromotedInSecondRowType() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1, "one");
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.NULL)
                .row(2, null);
        RowsBuilder expected = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1, "one")
                .row(2, null);
        check(first, second, expected);
    }

    @Test
    public void nullPromotedInFirstRowType() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.NULL)
                .row(1, null);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(2, "two");
        RowsBuilder expected = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(1, null)
                .row(2, "two");
        check(first, second, expected);
    }

    @Test
    public void twoOpens() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG)
                .row(1L);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG)
                .row(2L);
        Operator union = union(first, second);
        Cursor cursor = OperatorTestHelper.open(union);
        int count = 0;
        while(cursor.next() != null) {
            ++count;
        }
        assertEquals("count", 2, count);
        cursor.close();
        count = 0;
        OperatorTestHelper.reopen(cursor);
        while(cursor.next() != null) {
            ++count;
        }
        assertEquals("count", 2, count);
    }

    @Test
    public void bothInputsEmpty() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder expected = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        check(first, second, expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void inputsNotOfRightShape() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.TEXT);
        union(first, second);
    }

    @Test(expected = IllegalArgumentException.class)
    public void firstOperatorIsNull() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.NULL);
        new UnionAll_Default(null, first.rowType(), new TestOperator(second), second.rowType());
    }

    @Test(expected = NullPointerException.class)
    public void firstRowTypeIsNull() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.NULL);
        new UnionAll_Default(new TestOperator(first), null, new TestOperator(second), second.rowType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void secondOperatorIsNull() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.NULL);
        new UnionAll_Default(new TestOperator(first), first.rowType(), null, second.rowType());
    }

    @Test(expected = NullPointerException.class)
    public void secondRowTypeIsNull() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.NULL);
        new UnionAll_Default(new TestOperator(first), first.rowType(), new TestOperator(second), null);
    }

    /**
     * Tests what happens when one of the input streams outputs a rowType other than what we promised it would.
     * To make this test a bit more interesting, the outputted row is actually of the same shape as the expected
     * results: it just has a different rowTypeId.
     */
    @Test(expected = UnionAll_Default.WrongRowTypeException.class)
    public void inputsContainUnspecifiedRows() {
        DerivedTypesSchema schema = new DerivedTypesSchema();
        RowsBuilder first = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);
        RowsBuilder second = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR);

        RowsBuilder anotherStream = new RowsBuilder(schema, AkType.LONG, AkType.VARCHAR)
                .row(3, "three");
        first.rows().push(anotherStream.rows().pop());

        Operator union = union(first, second);
        OperatorTestHelper.execute(union);
    }

    private static void check(RowsBuilder rb1, RowsBuilder rb2, RowsBuilder expected) {
        Operator union = union(rb1, rb2);
        final RowType outputRowType = union.rowType();
        checkRowTypes(expected.rowType(), outputRowType);

        OperatorTestHelper.check(union, expected.rows(), new OperatorTestHelper.RowCheck() {
            @Override
            public void check(Row row) {
                assertEquals("row types", outputRowType, row.rowType());
            }
        });
    }

    private static void checkRowTypes(RowType expected, RowType actual) {
        assertEquals("number of fields", expected.nFields(), actual.nFields());
        for (int i=0; i < expected.nFields(); ++i) {
            assertEquals("field " + i, expected.typeAt(i), actual.typeAt(i));
        }
    }

    private static Operator union(RowsBuilder rb1, RowsBuilder rb2) {
        return new UnionAll_Default(
                    new TestOperator(rb1),
                    rb1.rowType(),
                    new TestOperator(rb2),
                    rb2.rowType()
            );
    }
}
