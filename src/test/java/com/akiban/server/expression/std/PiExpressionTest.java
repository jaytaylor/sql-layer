/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.Expression;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.TrigExpression.*;

public class PiExpressionTest 
{
    @Test
    public void test()
    {
        test(COS_COMPOSER, -1.0);
        test(SIN_COMPOSER, 0.0);
        test(TAN_COMPOSER, 0.0);
        test(COT_COMPOSER, Math.cos(Math.PI) / Math.sin(Math.PI));
    }
    
    public void test (ExpressionComposer c, double expected)
    {
        assertEquals( c.toString() + "(PI()) ", 
                expected, 
                c.compose(Arrays.asList(PiExpression.COMPOSER.compose(new ArrayList<Expression>()))).evaluation().eval().getDouble(), 
                0.0001);
    }
}
