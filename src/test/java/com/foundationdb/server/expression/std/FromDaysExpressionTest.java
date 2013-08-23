/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.expression.std;

import org.junit.Test;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;

import com.foundationdb.server.types.extract.Extractors;
import static org.junit.Assert.*;

public class FromDaysExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        doTest(-1L, "0000-00-00");
        doTest(null, null);
        doTest(1L, "0000-00-00");
        doTest(10L, "0000-00-00");
        doTest(365L, "0000-00-00");
        doTest(366L, "0001-01-01");
        
        doTest(715875L, "1960-01-01");
        doTest(719528L, "1970-01-01");
        doTest(734980L, "2012-04-22");
    }
    
    private static void doTest(Long days, String date)
    {
        Expression top = new FromDaysExpression(days == null 
                ? LiteralExpression.forNull()
                : new LiteralExpression(AkType.LONG, days.longValue()));
        
        String name = "FROM_DAYS(" + days + ")";
        if (date == null)
            assertTrue(name + " Top should NULL", top.evaluation().eval().isNull());
        else
            assertEquals(name, date, Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.LONG, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return FromDaysExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
