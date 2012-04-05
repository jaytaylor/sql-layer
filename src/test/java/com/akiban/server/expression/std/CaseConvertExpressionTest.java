/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import java.util.Arrays;
import java.util.List;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.OnlyIf;
import com.akiban.server.types.ValueSourceIsNullException;
import org.junit.runner.RunWith;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public class CaseConvertExpressionTest extends ComposedExpressionTestBase
{
    private String input;
    private String expected;
    private CaseConvertExpression.ConversionType convertType;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.VARCHAR, true);

    private static boolean alreadyExecuted = false;
    
    public CaseConvertExpressionTest (String input, String expected, 
            CaseConvertExpression.ConversionType convertType)
    {
        this.input = input;
        this.expected = expected;
        this.convertType = convertType;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
       
        //------------TO LOWER TESTS--------------------------------------------
        // lowercase -> upper case
        param(pb, "lowercase string", "LOWERCASE STRING",
                CaseConvertExpression.ConversionType.TOUPPER);
        
        // null string -> upper
       param(pb, "", "", CaseConvertExpression.ConversionType.TOUPPER);
        
        // upper -> upper
        param(pb, "UPPERCASE", "UPPERCASE",
                CaseConvertExpression.ConversionType.TOUPPER);
        
        // null -> null : expect ValueSourceIsNull exception
        param(pb, null, null, CaseConvertExpression.ConversionType.TOUPPER);
        
        
        // --------------TOUPPER TESTS------------------------------------------
        // uppper -> lower
        param(pb, "UPPERCASE", "uppercase",
                CaseConvertExpression.ConversionType.TOLOWER);
        
        // null -> lower
        param(pb, "", "", CaseConvertExpression.ConversionType.TOLOWER);
        
        // lower -> lower
        param(pb, "lowercase", "lowercase",
                CaseConvertExpression.ConversionType.TOLOWER);  
      
        // null -> null : expect ValueSourceIsNull exception
        param(pb, null, null, CaseConvertExpression.ConversionType.TOLOWER);
        
        return pb.asList();
    }
    
     private static void param(ParameterizationBuilder pb,String input, String expected,
             CaseConvertExpression.ConversionType convertType)
     {
         pb.add(convertType.name() + input, input, expected, convertType);
         
     }
     
      
     @OnlyIfNot ("isAlreadyExecuted()")
     @Test (expected=WrongExpressionArityException.class)
     public void illegalArgTest ()
     {
        // excessive arguments
        getComposer().compose(getArgList(ExprUtil.lit("String 1"), ExprUtil.lit("String 2")));
       
        // insufficent arguments
        getComposer().compose(getArgList());
        
        // null argument
        getComposer().compose(getArgList(ExprUtil.constNull(AkType.VARCHAR)));            
     }
    
     @OnlyIf("expectNullException()")
     @Test(expected=ValueSourceIsNullException.class)
     public void expectingExceptionTest() {
         test();
     }
          
     @OnlyIfNot("expectNullException()")
     @Test()
     public void notExpectingExceptionTest() {
         test();
     }
      
    @Override
    protected ExpressionComposer getComposer() 
    {
        return (convertType == CaseConvertExpression.ConversionType.TOLOWER ?
                CaseConvertExpression.TOLOWER_COMPOSER:
                CaseConvertExpression.TOUPPER_COMPOSER);
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    public boolean expectNullException() 
    {
        return (input == null);
    }
    
    public boolean isAlreadyExecuted()
    {
        return alreadyExecuted;
    }
    
    private static List<? extends Expression> getArgList (Expression...st)
    {
        return Arrays.asList(st);
    }
        
    private void test ()
    {
        Expression inputExp = new LiteralExpression(AkType.VARCHAR, input);
        Expression expression = new CaseConvertExpression(inputExp,
                convertType);
        ValueSource result = expression.evaluation().eval();
        String actual = result.getString();
        
        assertTrue("Actual equals expected", actual.equals(expected));
        alreadyExecuted = true;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExecuted;
    }
    
}
