
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class SubStringExpressionTest extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(3, AkType.VARCHAR, true);

    @Test (expected=WrongExpressionArityException.class)
    public void testIllegalArg() 
    {
        // excessive arguments
        compose(getComposer(), getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
        
        // insufficent arguments
        compose(getComposer(), getArgList());
        
        // null arguments
        compose(getComposer(),
                getArgList(ExprUtil.constNull(AkType.VARCHAR), 
                           ExprUtil.constNull(AkType.VARCHAR), ExprUtil.constNull(AkType.VARCHAR)));
    }
    
    @Test                  //  substr() with 2 args behaves properly in unit tests
    public void testBug ()                                // , but NOT in IT tests
    {
        Expression st = new LiteralExpression(AkType.VARCHAR, "Sakila");
        Expression from = new LiteralExpression(AkType.LONG, 1L);
        Expression len = new LiteralExpression(AkType.LONG, 6L);
        
        // test 2 args
        assertEquals("Sakila", compose(SubStringExpression.COMPOSER, Arrays.asList(st, from))
                .evaluation().eval().getString());
        
        // test 3 args
        assertEquals("Sakila", compose(SubStringExpression.COMPOSER, Arrays.asList(st, from, len))
                .evaluation().eval().getString());
        
    }
    
    @Test
    public void test ()
    {
        // test with 2 argument
        subAndCheck("abcef", "bcef", 2);
        subAndCheck("quadratically", "ratically", 5);
        subAndCheck("Sakila", "Sakila", 1);
        subAndCheck("1234", "1234", 1); // meaning from index 0 to the end
        subAndCheck("1234", "", 0); // index out of bound -> null
        subAndCheck("1234", "", 9);  
        subAndCheck("1234", "234", -3); // meanding from index 1
        subAndCheck ("", "", 5); // empty string -> empty string
        subAndCheck(null,null,4);
        
        // test with 3 argument
        subAndCheck ("1234", "4", -1, 2); // meaning from index 3 till 4
        subAndCheck ("1234", "4",  -1, 4); // meanding from index 3 til 4
        subAndCheck ("1234", "1234", -4, 4); // meaning from index 0 till inde 3
        subAndCheck ("", "", -5, 9); // empty string -> empty string
        subAndCheck ("1234", "", 1, -5); // length < 0 => empty string
    }
   

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return SubStringExpression.COMPOSER;
    }
    
    // ---------------------private methods ------------------------------------
    
    private static void subAndCheck (String st,String expected, int from )
    {
        check(substr(st,from), expected);
    }
    
    private static void subAndCheck (String st, String expected, int from, int length)
    {
        check(substr(st,from,length), expected);
    }
    
    private static void check (Expression inputExp, String expected)
    {
        ValueSource result = inputExp.evaluation().eval();
        if (expected == null)
            assertTrue("TOp should be null: ", result.isNull());
        else
            assertEquals ("Actual equals expected ", expected, result.getString());
    }
    
    private static List <? extends Expression> getArgList (Expression ...arg)
    {     
        return Arrays.asList(arg);
    }
    
    private static Expression substr(String st, int from)
    {
        return new SubStringExpression(getArgList(ExprUtil.lit(st), ExprUtil.lit(from)));
    }
    
    private static Expression substr(String st, int from, int length)
    {
        return new SubStringExpression(getArgList(ExprUtil.lit(st), 
                ExprUtil.lit(from), ExprUtil.lit(length)));
    }

    @Override
    public boolean alreadyExc()
    {
        return false;
    }
}
