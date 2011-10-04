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
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class CoalesceExpressionTest extends ComposedExpressionTestBase {

    @Test
    public void hmm, I need to merge from trunk

    @Test
    public void onlyNotNull() {
        check(new ValueHolder(AkType.LONG, 5), lit(5));
    }

    @Test
    public void onlyNull() {
        check(NullValueSource.only(), LiteralExpression.forNull());
    }

    @Test(expected = IllegalArgumentException.class)
    public void noArgs() {
        new CoalesceExpression(Collections.<Expression>emptyList());
    }

    // ComposedExpressionTestBase

    @Override
    protected int childrenCount() {
        return 2; // it's as good as any other number
    }

    @Override
    protected Expression getExpression(List<? extends Expression> children) {
        return new CoalesceExpression(children);
    }

    // for use in this class
    private void check(ValueSource expected, Expression... children) {
        ValueHolder expectedHolder = new ValueHolder(expected);
        ValueHolder actualHolder = new ValueHolder(new CoalesceExpression(Arrays.asList(children)).evaluation().eval());
        assertEquals(expectedHolder, actualHolder);
    }

    private Expression lit(String string) {
        return new LiteralExpression(AkType.VARCHAR, string);
    }

    private Expression lit(long value) {
        return new LiteralExpression(AkType.LONG, value);
    }

    private Expression litNull() {
        return LiteralExpression.forNull();
    }
}
