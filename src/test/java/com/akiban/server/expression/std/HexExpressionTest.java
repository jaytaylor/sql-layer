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

import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class HexExpressionTest
{
    @Test
    public void test()
    {
        Expression arg = lit("\n");
        Expression top = new HexExpression(arg);
        
        assertEquals("0A", top.evaluation().eval().getString());
    }
}
