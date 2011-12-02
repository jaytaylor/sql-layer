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

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
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

    @Test
    public void typeLength() {
        ExpressionType concatType = 
            getComposer().composeType(Arrays.asList(ExpressionTypes.varchar(6),
                                                    ExpressionTypes.varchar(10),
                                                    ExpressionTypes.varchar(4)));
        assertEquals(AkType.VARCHAR, concatType.getType());
        assertEquals(20, concatType.getPrecision());
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
