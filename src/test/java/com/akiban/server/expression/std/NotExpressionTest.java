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

import com.akiban.server.types.AkType;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class NotExpressionTest extends ComposedExpressionTestBase {

    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.BOOL, true);

    @Test
    public void notTrue() {
        assertEquals(Boolean.FALSE, getNot(true));
    }

    @Test
    public void notFalse() {
        assertEquals(Boolean.TRUE, getNot(false));
    }

    @Test
    public void notNull() {
        assertEquals(null, getNot(null));
    }

    // ComposedExpressionTestBase interface

    @Override
    protected CompositionTestInfo getTestInfo() {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() {
        return NotExpression.COMPOSER;
    }

    // private

    private static Boolean getNot(Boolean in) {
        return Extractors.getBooleanExtractor().getBoolean(
                new NotExpression(LiteralExpression.forBool(in)).evaluation().eval(),
                null
        );
    }

    @Override
    protected boolean alreadyExc()
    {
       return false;
    }
}
