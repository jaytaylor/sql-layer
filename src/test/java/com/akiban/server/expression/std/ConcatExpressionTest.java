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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.akiban.server.expression.std.ExprUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class ConcatExpressionTest  extends ComposedExpressionTestBase {

    @Test
    public void smoke() {
        concatAndCheck("foo 3 bar", lit("foo "), lit(3), lit(" bar"));
    }

    @Test
    public void contaminatingNull() {
        Expression concat = concat(lit("foo"), nonConstNull(AkType.VARCHAR), exploding(AkType.LONG));
        assertFalse("expression should be non-const", concat.isConstant());
        check(null, concat);
    }


    @Test
    public void allNumbers() {
        Expression concat = concat(lit(1), lit(2), lit(3.0));
        assertTrue("concat should be const", concat.isConstant());
        check("123.0", concat);
    }
  

    @Test
    public void nonConstNullStillConstConcat() {
        Expression concat = concat(lit(3), nonConst(3), LiteralExpression.forNull());
        assertTrue("concat should be const", concat.isConstant());
        check(null, concat);
    }

    @Test
    public void noChildren() {
        concatAndCheck("");
    }

    /**
     * The test is moved to com.akiban.sql.optimizer package 
     */
    @Test
    public void typeLength() {
//        ExpressionType concatType =
//            getComposer().composeType(Arrays.asList(ExpressionTypes.varchar(6),
//                                                    ExpressionTypes.varchar(10),
//                                                    ExpressionTypes.varchar(4)));
//        assertEquals(AkType.VARCHAR, concatType.getType());
//        assertEquals(20, concatType.getPrecision());
    }

    // ComposedExpressionTestBase
     

    @Override
    protected CompositionTestInfo getTestInfo() {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() {
        return new ConcatExpression.ConcatComposer() {
            @Override
            public Expression compose(List<? extends Expression> arguments) {
                return new ConcatExpression(arguments);
            }
        };
    }

    // use in this class
    private static void concatAndCheck(String expected, Expression... inputs) {
        check(expected, concat(inputs));
    }

    private static void check(String expected, Expression concatExpression) {
        ValueSource concatValue = concatExpression.evaluation().eval();
        if (expected == null) {
            AkType concatType = concatValue.getConversionType();
            assertTrue(
                    "type should have been null or VARCHAR, was " + concatType,
                    concatType == AkType.VARCHAR || concatType == AkType.NULL
            );
            assertTrue("result should have been null: " + concatValue, concatValue.isNull());
        }
        else {
            assertEquals("actual type", AkType.VARCHAR, concatValue.getConversionType());
            assertEquals("result", expected, concatValue.getString());
        }
    }

    private static Expression concat(Expression... inputs) {
        return new ConcatExpression(Arrays.asList(inputs));
    }

    private final CompositionTestInfo testInfo = new CompositionTestInfo(3, AkType.VARCHAR, true);

    @Override
    public boolean alreadyExc()
    {
        return false;
    }
}
