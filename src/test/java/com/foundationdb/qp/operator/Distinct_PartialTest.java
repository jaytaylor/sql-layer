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

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.OperatorTestHelper;
import com.foundationdb.qp.operator.RowsBuilder;
import com.foundationdb.qp.operator.TestOperator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.server.types3.texpressions.TPreparedField;
import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.*;

import com.foundationdb.server.types.AkType;

import org.junit.Test;

import java.util.Deque;

public class Distinct_PartialTest {

    @Test
    public void testDistinct() {
        Operator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",1L)
            .row(3L,"def",0L)
            .row(3L,"def",null)
        );
        Operator plan = distinct_Partial(input, input.rowType());
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(2L,"xyz",0L)
            .row(2L,"xyz",1L)
            .row(3L,"def",0L)
            .row(3L,"def",null)
            .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void testPartial() {
        Operator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
            .row(1L,"abc",0L)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(1L,"abc",0L)
        );
        Operator plan = distinct_Partial(input, input.rowType());
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.LONG)
            .row(1L,"abc",0L)
            .row(2L,"abc",0L)
            .row(1L,"abc",0L)
            .rows();
        OperatorTestHelper.check(plan, expected);
    }

    @Test
    public void testWithSort() {
        Operator input = new TestOperator(new RowsBuilder(AkType.LONG, AkType.VARCHAR)
            .row(7L,"def")
            .row(3L,"def")
            .row(0L,"abc")
            .row(9L,"xyz")
            .row(3L,"xyz")
            .row(2L,"xyz")
            .row(9L,"xyz")
            .row(0L,"abc")
            .row(2L,"xyz")
            .row(6L,"def")
            .row(7L,"def")
            .row(6L,"ghi")
            .row(2L,"abc")
            .row(6L,"def")
            .row(2L,"def")
            .row(7L,"def")
            .row(8L,"ghi")
            .row(2L,"abc")
            .row(5L,"abc")
            .row(9L,"ghi")
            .row(6L,"ghi")
            .row(5L,"xyz")
            .row(5L,"ghi")
            .row(3L,"def")
            .row(7L,"abc")
            .row(7L,"abc")
            .row(4L,"abc")
            .row(0L,"ghi")
            .row(9L,"def")
            .row(3L,"def")
            .row(7L,"xyz")
            .row(4L,"def")
            .row(4L,"ghi")
            .row(4L,"abc")
            .row(8L,"abc")
            .row(6L,"def")
            .row(3L,"def")
            .row(1L,"ghi")
            .row(4L,"abc")
            .row(8L,"abc")
            .row(3L,"ghi")
            .row(8L,"abc")
            .row(8L,"abc")
            .row(8L,"abc")
            .row(8L,"abc")
            .row(9L,"ghi")
            .row(9L,"xyz")
            .row(6L,"def")
            .row(3L,"def")
            .row(2L,"ghi")
            .row(2L,"abc")
            .row(8L,"abc")
            .row(5L,"ghi")
            .row(6L,"xyz")
            .row(7L,"xyz")
            .row(0L,"def")
            .row(6L,"def")
            .row(3L,"abc")
            .row(2L,"abc")
            .row(1L,"xyz")
            .row(9L,"ghi")
            .row(1L,"def")
            .row(4L,"ghi")
            .row(5L,"def")
            .row(0L,"def")
            .row(0L,"def")
            .row(1L,"ghi")
            .row(8L,"def")
            .row(4L,"abc")
            .row(6L,"xyz")
            .row(6L,"def")
            .row(0L,"xyz")
            .row(1L,"abc")
            .row(7L,"abc")
            .row(6L,"xyz")
            .row(8L,"ghi")
            .row(7L,"xyz")
            .row(6L,"xyz")
            .row(5L,"xyz")
            .row(5L,"ghi")
            .row(5L,"xyz")
            .row(4L,"def")
            .row(4L,"xyz")
            .row(8L,"def")
            .row(2L,"def")
            .row(3L,"abc")
            .row(3L,"def")
            .row(7L,"xyz")
            .row(2L,"xyz")
            .row(2L,"def")
            .row(5L,"ghi")
            .row(8L,"ghi")
            .row(0L,"def")
            .row(2L,"ghi")
            .row(1L,"xyz")
            .row(7L,"xyz")
            .row(1L,"abc")
            .row(4L,"abc")
            .row(9L,"def")
            .row(7L,"abc")
        );
        Ordering ordering = ordering();
        if (Types3Switch.ON) {
            ordering.append(new TPreparedField(input.rowType().typeInstanceAt(0), 0), true);
            ordering.append(new TPreparedField(input.rowType().typeInstanceAt(1), 1), true);
        }
        else {
            ordering.append(field(input.rowType(), 0), true);
            ordering.append(field(input.rowType(), 1), true);
        }
        Operator sort = sort_InsertionLimited(input, input.rowType(),
                                              ordering, SortOption.PRESERVE_DUPLICATES, 200);
        Operator plan = distinct_Partial(sort, sort.rowType());
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR)
            .row(0L,"abc")
            .row(0L,"def")
            .row(0L,"ghi")
            .row(0L,"xyz")
            .row(1L,"abc")
            .row(1L,"def")
            .row(1L,"ghi")
            .row(1L,"xyz")
            .row(2L,"abc")
            .row(2L,"def")
            .row(2L,"ghi")
            .row(2L,"xyz")
            .row(3L,"abc")
            .row(3L,"def")
            .row(3L,"ghi")
            .row(3L,"xyz")
            .row(4L,"abc")
            .row(4L,"def")
            .row(4L,"ghi")
            .row(4L,"xyz")
            .row(5L,"abc")
            .row(5L,"def")
            .row(5L,"ghi")
            .row(5L,"xyz")
            .row(6L,"def")
            .row(6L,"ghi")
            .row(6L,"xyz")
            .row(7L,"abc")
            .row(7L,"def")
            .row(7L,"xyz")
            .row(8L,"abc")
            .row(8L,"def")
            .row(8L,"ghi")
            .row(9L,"def")
            .row(9L,"ghi")
            .row(9L,"xyz")
            .rows();
        OperatorTestHelper.check(plan, expected);
    }

    // TODO: testCursor
}
