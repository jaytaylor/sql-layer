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

package com.akiban.server.expression.subquery;

import com.akiban.server.error.SubqueryTooManyRowsException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.OldExpressionTestBase;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.functions.FunctionsRegistryImpl;
import static com.akiban.server.expression.std.Expressions.*;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.OperatorTestHelper;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import static com.akiban.qp.operator.API.*;

import com.akiban.server.types.AkType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;

public class ScalarSubqueryExpressionTest extends OldExpressionTestBase {

    private FunctionsRegistry functionsRegistry = new FunctionsRegistryImpl();

    @Test
    public void testScalar() {
        Schema schema = OperatorTestHelper.schema();
        Operator outer = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
                .row((Long)null)
                                          
        );
        Operator inner = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
        );

        Expression equals = compare(boundField(outer.rowType(), 1, 0),
                                    Comparison.NE,
                                    field(inner.rowType(), 0));
        Operator innerPlan = select_HKeyOrdered(inner, inner.rowType(), equals);
        Expression expression = functionsRegistry
            .composer("plus")
            .compose(Arrays.asList(boundField(outer.rowType(), 1, 0),
                                   field(inner.rowType(), 0)),
                     Collections.<ExpressionType>nCopies(3, null));
        Expression scalar = new ScalarSubqueryExpression(innerPlan, expression,
                                                         outer.rowType(), inner.rowType(), 1);
        Expression outerN = field(outer.rowType(), 0);
        Operator outerPlan = project_Default(outer, outer.rowType(),
                                             Arrays.asList(outerN, scalar));
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.LONG)
            .row(1, 3)
            .row(2, 3)
            .row((Long)null, (Long)null)
            .rows();
        OperatorTestHelper.check(outerPlan, expected);
    }

    @Test(expected = SubqueryTooManyRowsException.class)
    public void testTooManyRows() {
        Schema schema = OperatorTestHelper.schema();
        Operator outer = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
                .row(3L)
                .row((Long)null)
                                          
        );
        Operator inner = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
        );

        Expression equals = compare(boundField(outer.rowType(), 1, 0),
                                    Comparison.NE,
                                    field(inner.rowType(), 0));
        Operator innerPlan = select_HKeyOrdered(inner, inner.rowType(), equals);
        Expression expression = functionsRegistry
            .composer("plus")
            .compose(Arrays.asList(boundField(outer.rowType(), 1, 0),
                                   field(inner.rowType(), 0)),
                     Collections.<ExpressionType>nCopies(3, null));
        Expression scalar = new ScalarSubqueryExpression(innerPlan, expression,
                                                         outer.rowType(), inner.rowType(), 1);
        Expression outerN = field(outer.rowType(), 0);
        Operator outerPlan = project_Default(outer, outer.rowType(),
                                             Arrays.asList(outerN, scalar));
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.LONG)
            .row(1, 3)
            .row(2, 3)
            .row(3, 4)
            .row((Long)null, (Long)null)
            .rows();
        OperatorTestHelper.check(outerPlan, expected);
    }

}
