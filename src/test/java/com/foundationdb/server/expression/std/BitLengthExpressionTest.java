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

import java.io.IOException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import org.junit.Test;
import static org.junit.Assert.*;

public class BitLengthExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test() throws IOException
    {
        // test regular string
        test("abc", 24, "UTF-8" ); // utf-8 is used as charset

        // test unicode string
        test("♫♠", 48, "UTF-8");

        // test empty string
        test("", 0, "UTF-8");

        // test null
        test (null, -1, "UTF-8");
    }
    
    private static void test(String input, long expected, String charset)
    {
        Expression ex;
        if (input == null)
            ex = LiteralExpression.forNull();
        else
            ex = new BitLengthExpression ( new LiteralExpression(AkType.VARCHAR, input), charset);

        ValueSource eval = ex.evaluation().eval();

        if (expected < 0)
            assertTrue(".eval is null", eval.isNull());
        else
            assertEquals("bit_leng(" + input + ")",expected, eval.getLong());
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return BitLengthExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
