
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
