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
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

public class TestNull 
{
    @Test
    public void test_forNullEqualsNewLiteralNull()
    {
        Expression litNull = LiteralExpression.forNull();
        Expression nullExp = new LiteralExpression(AkType.NULL, null);
        assertSame(litNull.evaluation(), nullExp.evaluation());
    }
    
    @Test
    public void test_LiteralForNullEvalEqualNullValueSouce ()
    {
        assertEquals(LiteralExpression.forNull().evaluation().eval(), NullValueSource.only());
    }
}
