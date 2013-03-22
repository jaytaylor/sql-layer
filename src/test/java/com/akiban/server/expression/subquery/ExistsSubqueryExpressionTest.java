
package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.server.expression.OldExpressionTestBase;
import com.akiban.server.expression.std.Comparison;
import static com.akiban.server.test.ExpressionGenerators.*;

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
import java.util.Deque;

public class ExistsSubqueryExpressionTest extends OldExpressionTestBase {

    @Test
    public void testExists() {
        Schema schema = OperatorTestHelper.schema();
        Operator outer = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
                .row(3L)
        );
        Operator inner = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(2L)
                .row(1L)
        );

        ExpressionGenerator equals = compare(boundField(outer.rowType(), 1, 0),
                                    Comparison.EQ,
                                    field(inner.rowType(), 0), castResolver());
        Operator innerPlan = select_HKeyOrdered(inner, inner.rowType(), equals);
        ExpressionGenerator exists = existsSubquery(innerPlan, outer.rowType(),
                                                         inner.rowType(), 1);
        ExpressionGenerator outerN = field(outer.rowType(), 0);
        Operator outerPlan = project_Default(outer, outer.rowType(),
                                             Arrays.asList(outerN, exists));
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.BOOL)
            .row(1, true)
            .row(2, true)
            .row(3, false)
            .rows();
        OperatorTestHelper.check(outerPlan, expected);
    }

}
