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

package com.akiban.qp.operator;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.OperatorTestHelper;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.texpressions.TPreparedField;
import static com.akiban.qp.operator.API.*;
import static com.akiban.server.expression.std.Expressions.*;

import com.akiban.server.types.AkType;

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
            ordering.append(null, new TPreparedField(input.rowType().typeInstanceAt(0), 0), true);
            ordering.append(null, new TPreparedField(input.rowType().typeInstanceAt(1), 1), true);
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
