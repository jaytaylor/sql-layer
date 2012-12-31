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

package com.akiban.server.expression.std;

import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class FieldExpressionTest {
    @Test
    public void twoRows() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, AkType.LONG);
        Expression fieldExpression = new FieldExpression(dummyType, 0);

        assertFalse("shouldn't be constant", fieldExpression.isConstant());
        assertEquals("type", AkType.LONG, fieldExpression.valueType());
        ExpressionEvaluation evaluation = fieldExpression.evaluation();

        evaluation.of(new ValuesRow(dummyType, new Object[]{27L}));
        assertEquals("evaluation.eval()", new ValueHolder(AkType.LONG, 27L), new ValueHolder(evaluation.eval()));

        evaluation.of(new ValuesRow(dummyType, new Object[]{23L}));
        assertEquals("evaluation.eval()", new ValueHolder(AkType.LONG, 23L), new ValueHolder(evaluation.eval()));
    }

    @Test(expected = IllegalStateException.class)
    public void noRows() {
        final ExpressionEvaluation evaluation;
        try {
            ValuesRowType dummyType = new ValuesRowType(null, 1, AkType.LONG);
            Expression fieldExpression = new FieldExpression(dummyType, 0);
            assertEquals("type", AkType.LONG, fieldExpression.valueType());
            evaluation = fieldExpression.evaluation();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        evaluation.eval();
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongRow() {
        final ExpressionEvaluation evaluation;
        final Row badRow;
        try {
            ValuesRowType dummyType1 = new ValuesRowType(null, 1, AkType.LONG);
            evaluation = new FieldExpression(dummyType1, 0).evaluation();
            ValuesRowType dummyType2 = new ValuesRowType(null, 2, AkType.LONG); // similar, but not same!
            badRow = new ValuesRow(dummyType2, new Object[] { 31L });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        evaluation.of(badRow);
    }

    @Test(expected = IllegalArgumentException.class)
    public void indexTooLow() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, AkType.LONG);
        new FieldExpression(dummyType, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void indexTooHigh() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, AkType.LONG);
        new FieldExpression(dummyType, 1);
    }

    @Test(expected = AkibanInternalException.class)
    @Ignore
    public void wrongFieldType() {
        final ExpressionEvaluation evaluation;
        final Row badRow;
        try {
            ValuesRowType dummyType = new ValuesRowType(null, 1, AkType.LONG);
            evaluation = new FieldExpression(dummyType, 0).evaluation();
            badRow = new ValuesRow(dummyType, new Object[] { 31.4159 });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        evaluation.of(badRow);
    }

    @Test(expected = NullPointerException.class)
    public void nullRowType() {
        new FieldExpression(null, 0);
    }
    
    @Test
    public void testSharing() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, AkType.LONG);
        ExpressionEvaluation evaluation = new FieldExpression(dummyType, 0).evaluation();

        ValuesRow row = new ValuesRow(dummyType, new Object[]{27L});
        evaluation.of(row);

        assertEquals("evaluation.isShared()", false, evaluation.isShared());
        assertEquals("row.isShared", false, row.isShared());

        // first acquire doesn't mean it's shared
        evaluation.acquire();
        assertEquals("evaluation.isShared()", false, evaluation.isShared());
        assertEquals("row.isShared", false, row.isShared());

        // next does
        evaluation.acquire();
        assertEquals("evaluation.isShared()", true, evaluation.isShared());
        assertEquals("row.isShared", true, row.isShared());

        // now, three own it (very shared!)
        evaluation.acquire();
        assertEquals("evaluation.isShared()", true, evaluation.isShared());
        assertEquals("row.isShared", true, row.isShared());

        // back down to two owners, still shared
        evaluation.release();
        assertEquals("evaluation.isShared()", true, evaluation.isShared());
        assertEquals("row.isShared", true, row.isShared());

        // down to one owner, not shared anymore
        evaluation.release();
        assertEquals("evaluation.isShared()", false, evaluation.isShared());
        assertEquals("row.isShared", false, row.isShared());

        // no owners, very not shared
        evaluation.release();
        assertEquals("evaluation.isShared()", false, evaluation.isShared());
        assertEquals("row.isShared", false, row.isShared());
    }
}
