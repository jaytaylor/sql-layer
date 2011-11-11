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
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

import static com.akiban.server.expression.std.ExprUtil.*;

public final class CoalesceExpressionTest extends ComposedExpressionTestBase {

    @Test
    public void smoke() {
        check(new ValueHolder(AkType.LONG, 5), constNull(), constNull(), lit(5), exploding(AkType.LONG));
    }

    @Test
    public void onlyNotNull() {
        check(new ValueHolder(AkType.LONG, 5), lit(5));
    }

    @Test
    public void onlyNull() {
        check(NullValueSource.only(), LiteralExpression.forNull());
    }

    @Test
    public void typedNull() {
        check(NullValueSource.only(), AkType.LONG, constNull(AkType.LONG));
    }
    @Test
    public void heterogeneousInputs() {
        check(new ValueHolder(AkType.VARCHAR, "3"), constNull(), constNull(AkType.VARCHAR), lit(3), lit("hello"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void noArgs() {
        new CoalesceExpression(Collections.<Expression>emptyList());
    }

    // ComposedExpressionTestBase

    @Override
    protected ExpressionComposer getComposer() {
        return CoalesceExpression.COMPOSER;
    }

    @Override
    protected CompositionTestInfo getTestInfo () {
        return testInfo;
    }

    // for use in this class
    private void check(ValueSource expected, AkType expectedType, Expression... children) {
        Expression coalesceExpression = new CoalesceExpression(Arrays.asList(children));
        assertEquals(expectedType, coalesceExpression.valueType());
        ValueHolder expectedHolder = new ValueHolder(expected);
        ValueHolder actualHolder = new ValueHolder(coalesceExpression.evaluation().eval());
        assertEquals(expectedHolder, actualHolder);
    }

    private void check(ValueSource expected, Expression... children) {
        check(expected, expected.getConversionType(), children);
    }

    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.LONG, false);
}
