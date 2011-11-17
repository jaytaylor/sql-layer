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
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

import com.akiban.server.types.conversion.Converters;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class IfExpressionTest extends ComposedExpressionTestBase
{
    private static final CompositionTestInfo testInfo = new CompositionTestInfo(3, AkType.LONG, false);

    @Test
    public void test ()
    {
       Expression cond = new LiteralExpression(AkType.BOOL, true);
       Expression trueExp = new LiteralExpression(AkType.LONG, 1L);
       Expression falseExp = new LiteralExpression(AkType.LONG, 2L);
       
       Expression ifExp = new IfExpression(Arrays.asList(cond, trueExp, falseExp));
       
       assertTrue(ifExp.evaluation().eval().getLong() == 1L);
       
        
    }
    

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

  @Override
    protected ExpressionComposer getComposer()
    {
        return IfExpression.COMPOSER;
    }
   
}
