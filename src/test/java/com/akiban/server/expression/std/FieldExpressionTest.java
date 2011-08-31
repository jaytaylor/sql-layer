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
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class FieldExpressionTest {
    @Test
    public void twoRows() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, 1);
        Expression fieldExpression = new FieldExpression(dummyType, 0, AkType.LONG);

        assertFalse("shouldn't be constant", fieldExpression.isConstant());
        assertEquals("type", AkType.LONG, fieldExpression.valueType());
        ExpressionEvaluation evaluation = fieldExpression.evaluation();

        evaluation.of(new ValuesRow(dummyType, new Object[]{27L}), UndefBindings.only());
        assertEquals("evaluation.eval()", new ValueHolder(AkType.LONG, 27L), new ValueHolder(evaluation.eval()));

        evaluation.of(new ValuesRow(dummyType, new Object[]{23L}), UndefBindings.only());
        assertEquals("evaluation.eval()", new ValueHolder(AkType.LONG, 23L), new ValueHolder(evaluation.eval()));
    }

    @Test(expected = IllegalStateException.class)
    public void noRows() {
        final ExpressionEvaluation evaluation;
        try {
            ValuesRowType dummyType = new ValuesRowType(null, 1, 1);
            Expression fieldExpression = new FieldExpression(dummyType, 0, AkType.LONG);
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
            ValuesRowType dummyType1 = new ValuesRowType(null, 1, 1);
            evaluation = new FieldExpression(dummyType1, 0, AkType.LONG).evaluation();
            ValuesRowType dummyType2 = new ValuesRowType(null, 2, 1); // similar, but not same!
            badRow = new ValuesRow(dummyType2, new Object[] { 31L });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        evaluation.of(badRow, UndefBindings.only());
    }

    @Test(expected = IllegalArgumentException.class)
    public void indexTooLow() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, 1);
        new FieldExpression(dummyType, -1, AkType.LONG);
    }

    @Test(expected = IllegalArgumentException.class)
    public void indexTooHigh() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, 1);
        new FieldExpression(dummyType, 1, AkType.LONG);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongFieldType() {
        final ExpressionEvaluation evaluation;
        final Row badRow;
        try {
            ValuesRowType dummyType = new ValuesRowType(null, 1, 1);
            evaluation = new FieldExpression(dummyType, 0, AkType.LONG).evaluation();
            badRow = new ValuesRow(dummyType, new Object[] { 31.4159 });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        evaluation.of(badRow, UndefBindings.only());
    }

    @Test(expected = NullPointerException.class)
    public void nullRowType() {
        new FieldExpression(null, 0, AkType.LONG);
    }

    @Test(expected = IllegalArgumentException.class)
    public void nullAkType() {
        ValuesRowType dummyType = new ValuesRowType(null, 1, 1);
        new FieldExpression(dummyType, 0, null);
    }
}
