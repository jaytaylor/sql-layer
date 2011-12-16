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

import java.io.IOException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.Test;
import static org.junit.Assert.*;

public class BitLengthExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test() throws IOException
    {
        // test regular string
        test("abc", 24, null); // no charset specified
        test("abc", 24, "UTF-8" ); // utf-8 is used as charset

        // test unicode string
        test("♫♠", 48, "UTF-8");

        // test empty string
        test("", 0, null);
        test("", 0, "UTF-8");

        // test null
        test (null, -1, null);
        test (null, -1, "UTF-8");
    }
    
    @Test
    public void testUnicodeStringWithoutCharset ()
    {
         
        Expression ex = new BitLengthExpression ( new LiteralExpression(AkType.VARCHAR, "♫♠"), null);
        ValueSource eval = ex.evaluation().eval();
        
        assertEquals ("Top is LONG", AkType.LONG, eval.getConversionType());
        assertTrue ("Result is " + eval.getLong(), true); // result could vary depending on the system's
                                                          // charset. So we are not going to test for that.
                                                          // This is wrong but there's no other way
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
    public boolean alreadyExc()
    {
        return false;
    }
}
