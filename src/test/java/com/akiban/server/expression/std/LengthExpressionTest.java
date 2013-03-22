

package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import java.util.List;
import com.akiban.server.types.ValueSourceIsNullException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class LengthExpressionTest extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.LONG, true);

    public LengthExpressionTest ()
    {
        super();
    }

    @Test (expected=ValueSourceIsNullException.class)
    public  void test()
    {
        String st;
        
        testLength (st = "String 1", (long)st.length());
        testLength (st = "", (long)st.length());
        testLength (st = null, 0);
    }
    
    @Test (expected=WrongExpressionArityException.class)
    public void testIllegalArg() 
    {
        // excessive arguments
        compose(getComposer(), getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
       
        // insufficent arguments
        compose(getComposer(), getArgList());
        
        // null arguments
        compose(getComposer(), getArgList(ExprUtil.constNull(AkType.VARCHAR)));
    }
    
   
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return LengthExpression.COMPOSER;
    }
    
    
    private static List<? extends Expression> getArgList (Expression...st)
    {
        return Arrays.asList(st);
    }
    
    private  void testLength (String input, long expected)
    {
        Expression inputExp = new LiteralExpression(AkType.VARCHAR, input);
        Expression expression = new LengthExpression (inputExp); 
        ValueSource source = expression.evaluation().eval();
       
        long actual = source.getLong();    
        assertEquals(expected, actual);
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }

}
