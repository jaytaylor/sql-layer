
package com.akiban.server.expression.std;

import java.util.Arrays;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.*;

public class ReplaceExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void test()
    {
        //SELECT REPLACE('www.mysql.com', 'w', 'Ww');
        test("www.mysql.com", "w", "Ww", "WwWwWw.mysql.com");

        // SELECT REPLACE ('', 'a', 'b');
        test("", "a", "b", "");

        // SELECT REPLACE ('a', '', 'b');
        test("a", "", "b", "a");

        //SELECT REPLACE ('abcd', 'a', '');
        test("abcd", "a", "", "bcd");

        //SELECT REPLACE ('', '','');
        test("", "", "", "");

        // SELECT REPLACE ('abc', 'x', 'y');
        test("abc", "x", "y", "abc");

        // SELECT REPLACE (NULL, 'a', 'b');
        test(null, "a", "b", null);

        // SELECT REPLACE ('abc', NULL, 'b');
        test("abc", null, "b", null);

        // SELECT REPLACE ('abc', 'b', null);
        test("abc", "b", null, null);

        // SELECT REPLACE (NULL, NULL, NULL);
        test(null, null, null, null);
    }

    private static void test (String st1, String st2, String st3, String expected)
    {
        Expression top = new ReplaceExpression(Arrays.asList(getExp(st1), getExp(st2), getExp(st3)));
        ValueSource source = top.evaluation().eval();

        if (expected == null)
            assertTrue(".eval is null", source.isNull());
        else
            assertEquals (expected, source.getString());
    }

    private static Expression getExp(String st)
    {
        if (st == null) return LiteralExpression.forNull();
        else return new LiteralExpression(AkType.VARCHAR, st);
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ReplaceExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
