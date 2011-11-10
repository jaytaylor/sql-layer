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
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public final class UnionAll_DefaultTest {
    
    @Test
    public void unionTwoNormal() {
        RowsBuilder first = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one");
        RowsBuilder second = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(3L, "three")
                .row(1L, "one")
                .row(2L, "deux");
        RowsBuilder expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
                .row(1L, "one")
                .row(2L, "two")
                .row(1L, "one")
                .row(3L, "three")
                .row(1L, "one")
                .row(2L, "deux");
        check(first, second, expected.rows());
    }

    @Test
    public void skipEmptyInputs() {
        stub();
    }

    @Test
    public void noInputs() {
        stub();
    }

    private static void check(RowsBuilder rb1, RowsBuilder rb2, Collection<? extends Row> expecteds) {
        Operator union = new UnionAll_Default(
                new TestOperator(rb1),
                rb1.rowType(),
                new TestOperator(rb2),
                rb2.rowType()
        );
        final RowType outputRowType = union.rowType();
        OperatorTestHelper.check(union, expecteds, new OperatorTestHelper.RowCheck() {
            @Override
            public void check(Row row) {
                assertEquals("row types", outputRowType, row.rowType());
            }
        });
    }

    private static void stub() {
        throw new UnsupportedOperationException();
    }
}
