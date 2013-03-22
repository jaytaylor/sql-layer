
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import junit.framework.Assert;
import org.junit.Test;


public class SubstringIndexExpressionTest extends ComposedExpressionTestBase 
{
     
    @Test
    public void testWraparound()
    {
        Expression num = new LiteralExpression(AkType.LONG, -1);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("bacbacbab321zbx"), 
                getExp("bacbacbab"), 
                num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("321zbx", source.getString());
    }
      
    @Test
    public void testRegular()
    {
        Expression num = new LiteralExpression(AkType.LONG, 2);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("."), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.mysql", source.getString());
    }
    
    @Test
    public void testFull()
    {
        Expression num = new LiteralExpression(AkType.LONG, 3);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("."), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.mysql.com", source.getString());
    }
    
    @Test
    public void testMoreThanFull()
    {
     Expression num = new LiteralExpression(AkType.LONG, 4);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("."), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.mysql.com", source.getString());   
    }
    
    @Test
    public void testRegularNeg() 
    {
        Expression num = new LiteralExpression(AkType.LONG, -2);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("."), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("mysql.com", source.getString());
    }
    
    @Test
    public void testFullNeg()
    {
        Expression num = new LiteralExpression(AkType.LONG, -3);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("."), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.mysql.com", source.getString());
    }
    
    @Test
    public void testMoreThanFullNeg()
    {
        Expression num = new LiteralExpression(AkType.LONG, -4);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("."), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.mysql.com", source.getString());
    }
    
    @Test
    public void testLongDelimiter()
    {
        Expression num = new LiteralExpression(AkType.LONG, 1);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("my"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.", source.getString());
    }
    
    @Test
    public void testLongDelimiterNeg()
    {
        Expression num = new LiteralExpression(AkType.LONG, -1);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("my"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("sql.com", source.getString());
    }
    
    @Test
    public void testLongDelimiterNeg2()
    {
        Expression num = new LiteralExpression(AkType.LONG, -1);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.wysql.com"), getExp("my"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("www.wysql.com", source.getString());
    }
    
    @Test
    public void testDuplicate()
    {
        Expression num = new LiteralExpression(AkType.LONG, 2);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("w"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("w", source.getString());
    }
    
    @Test
    public void testNull()
    {
        Expression arg = LiteralExpression.forNull();
        Expression top = new SubstringIndexExpression(Arrays.asList(
                arg, arg, arg));
        
        Assert.assertTrue(top.evaluation().eval().isNull());
    }
    
    @Test
    public void testReturnEarly()
    {
        Expression num = new LiteralExpression(AkType.LONG, -2);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("xbz123babcabcabbabcabcab"), getExp("babcabcab"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("babcabcab", source.getString());
    }
    
    @Test
    public void testEmpty()
    {
        Expression num = new LiteralExpression(AkType.LONG, 0);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("www.mysql.com"), getExp("my"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("", source.getString());
    }
    
    @Test
    public void testNegCount()
    {
        Expression num = new LiteralExpression(AkType.LONG, -1);
        Expression top = new SubstringIndexExpression(Arrays.asList(
                getExp("bacbacbab321zbx"), getExp("bacbacbab"), num));
        ValueSource source = top.evaluation().eval();
        
        Assert.assertEquals("321zbx", source.getString());
    }
  
    private static Expression getExp(String str)
    {
        return new LiteralExpression(AkType.VARCHAR, str);
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return SubstringIndexExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }

}
