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

import org.junit.runner.RunWith;
import com.akiban.server.types.AkType;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.ValueSource;
import java.util.Collection;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public class TrimExpressionTest extends ComposedExpressionTestBase
{
    private String input;
    private String trimChar;
    private String expected;
    private TrimExpression.TrimType trimType;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.VARCHAR, true);

    private static boolean alreadyExc = false;
    public TrimExpressionTest (String input, String expected, 
            TrimExpression.TrimType trimType)
    {
        this.input = input;
        this.expected = expected;
        this.trimType = trimType;
        trimChar = " ";
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
        
        // trim leading
        param(pb, "     leading  ", "leading  ", TrimExpression.TrimType.LEADING);
        param(pb, "       ", "", TrimExpression.TrimType.LEADING);
        param(pb, "", "", TrimExpression.TrimType.LEADING);
        param(pb, "l     eading ", "l     eading ", TrimExpression.TrimType.LEADING);
        
        // trim trailing
        param(pb, " trailing  ", " trailing", TrimExpression.TrimType.TRAILING);
        param(pb, "   ", "", TrimExpression.TrimType.TRAILING);
        param(pb, "trailin    g", "trailin    g", TrimExpression.TrimType.TRAILING );
        
        
        // trim both
        param(pb, "  trim both   ", "trim both", null);
        param(pb, "", "", null);
        param(pb, "  leading", "leading", null);
        param(pb, "trailing  ", "trailing", null);
       
        
        return pb.asList();
    }
    
     private static void param(ParameterizationBuilder pb,String input, String expected,
            TrimExpression.TrimType trimType)
     {
         pb.add((trimType != null? trimType.name() : "TRIM") + input, input, expected, trimType);
         
     }
     
    
    @Test
    public void test ()
    {
        Expression inputExp = new LiteralExpression(AkType.VARCHAR, input);
        Expression trimCharExp = new LiteralExpression (AkType.VARCHAR, trimChar);
        Expression expression = new TrimExpression(inputExp, trimCharExp, trimType);
        ValueSource result = expression.evaluation().eval();
        String actual = result.getString();
        
        assertTrue("Actual equals expected", actual.equals(expected));
        alreadyExc = true;
    }
    
    
    //private void check

    @Override
    protected CompositionTestInfo getTestInfo ()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return (trimType == null?  
                TrimExpression.TRIM_COMPOSER :
                    (trimType == TrimExpression.TrimType.TRAILING ? TrimExpression.RTRIM_COMPOSER : 
                        TrimExpression.LTRIM_COMPOSER));
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
    
}
