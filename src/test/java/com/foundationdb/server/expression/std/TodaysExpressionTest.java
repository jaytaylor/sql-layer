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

import com.foundationdb.server.expression.Expression;
import org.junit.Test;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;

import com.foundationdb.server.types.extract.Extractors;
import java.util.Arrays;
import static org.junit.Assert.*;

public class TodaysExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        doTest("0001-01-01", 366L);
        doTest("0000-01-01", 0L);
        doTest("0000-01-02", 1L);
        doTest("0000-00-00", null);
    }
  
    
    private void doTest (String date, Long expected)
    {
        Expression top = compose(ToDaysExpression.COMPOSER, Arrays.asList( date == null
                ? LiteralExpression.forNull()
                : new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong(date))));
        
        String name = "TO_DAYS(" + date + ") ";
        if (expected == null)
            assertTrue(name + " TOP should be NULL", top.evaluation().eval().isNull());
        else
            assertEquals(name, expected.longValue(), top.evaluation().eval().getLong());
    }
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DATE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ToDaysExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
