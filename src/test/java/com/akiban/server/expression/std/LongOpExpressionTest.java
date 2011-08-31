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

import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class LongOpExpressionTest extends ComposedExpressionTestBase {

    @Test
    public void longMinusLong() {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        Expression right = new LiteralExpression(AkType.LONG, 2L);
        Expression top = new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left, right));
        
        assertTrue("top should be constant", top.isConstant());
        ValueSource actual = new ValueHolder(top.rowExpression().eval());
        ValueSource expected = new ValueHolder(LongOps.LONG_SUBTRACT.opType(), 3L);
        assertEquals("ValueSource", expected, actual);
    }

    @Test
    public void longMinusFloat() {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        Expression right = new LiteralExpression(AkType.FLOAT, 2F);
        Expression top = new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left, right));

        assertTrue("top should be constant", top.isConstant());
        ValueSource actual = new ValueHolder(top.rowExpression().eval());
        ValueSource expected = new ValueHolder(LongOps.LONG_SUBTRACT.opType(), 3L);
        assertEquals("ValueSource", expected, actual);
    }

    @Test
    public void longMinusString() {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        Expression right = new LiteralExpression(AkType.VARCHAR, "2");
        Expression top = new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left, right));

        assertTrue("top should be constant", top.isConstant());
        ValueSource actual = new ValueHolder(top.rowExpression().eval());
        ValueSource expected = new ValueHolder(LongOps.LONG_SUBTRACT.opType(), 3L);
        assertEquals("ValueSource", expected, actual);
    }

    @Test
    public void longMinusNull() {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        Expression right = LiteralExpression.forNull();
        Expression top = new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left, right));

        assertTrue("top should be constant", top.isConstant());
        ValueSource actual = new ValueHolder(top.rowExpression().eval());
        ValueSource expected = ValueHolder.holdingNull();
        assertEquals("ValueSource", expected, actual);
    }

    @Test
    public void usingFieldExpressions() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, 2);
        Expression left = new FieldExpression(dummyType, 0, AkType.LONG);
        Expression right = new FieldExpression(dummyType, 1, AkType.DOUBLE);
        Expression top = new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left, right));

        assertFalse("top shouldn't be constant", top.isConstant());
        ExpressionEvaluation evaluation = top.rowExpression();
        ValuesRow row = new ValuesRow(dummyType, new Object[] {5L, 2.9} );
        evaluation.of(row, UndefBindings.only());
        ValueSource actual = new ValueHolder(evaluation.eval());
        ValueSource expected = new ValueHolder(LongOps.LONG_SUBTRACT.opType(), 3L);
        assertEquals("ValueSource", expected, actual);
    }

    @Test(expected = WrongExpressionArityException.class)
    public void oneArg() {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left));
    }

    @Test(expected = WrongExpressionArityException.class)
    public void threeArgs() {
        Expression left = new LiteralExpression(AkType.LONG, 5L);
        Expression right = new LiteralExpression(AkType.VARCHAR, "2");
        Expression extra = new LiteralExpression(AkType.LONG, 2L);
        new LongOpExpression(LongOps.LONG_SUBTRACT, Arrays.asList(left, right, extra));
    }

    @Override
    protected int childrenCount() {
        return 2;
    }

    @Override
    protected Expression getExpression(List<? extends Expression> children) {
        return new LongOpExpression(LongOps.LONG_SUBTRACT, children);
    }
}
