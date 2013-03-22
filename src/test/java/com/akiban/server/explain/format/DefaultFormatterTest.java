
package com.akiban.server.explain.format;

import com.akiban.server.explain.Explainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.std.ExpressionExplainer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.ArithOps;
import com.akiban.server.expression.std.FromUnixExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.expression.std.SubStringExpression;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.*;

public class DefaultFormatterTest 
{

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of Describe method, of class Format.
     */
    @Test
    public void testDescribe_Explainer() {
        System.out.println("describe");
        
        // copied from TreeFormatTest
        // parent expresion: 
        // SUBSTRING(FROM_UNIXTIME(123456 * 7 + 8, "%Y-%m-%d"), 9 + 10, 11)
        
        // literal(11)
        Expression lit_11 = new LiteralExpression(AkType.LONG, 11L);
        
        // literal(9)
        Expression lit_9 = new LiteralExpression(AkType.LONG, 9L);
        
        // literal(10)
        Expression lit_10 = new LiteralExpression(AkType.LONG, 10L);
        
        // literal(7)
        Expression lit_7 = new LiteralExpression(AkType.LONG, 7L);
        
        // literal 8
        Expression lit_8 = new LiteralExpression(AkType.LONG, 8L);
        
        // literal 123456
        Expression lit_123456 = new LiteralExpression(AkType.LONG, 123456L);
        
        // literal varchar exp
        Expression lit_varchar = new LiteralExpression(AkType.VARCHAR, "%Y-%m-%d");
        
        // from times exp
        Expression times = ((ExpressionComposer)ArithOps.MULTIPLY).compose(Arrays.asList(lit_123456, lit_7), Collections.<ExpressionType>nCopies(3, null));
        Expression add = ((ExpressionComposer)ArithOps.ADD).compose(Arrays.asList(times, lit_8), Collections.<ExpressionType>nCopies(3, null));
        Expression times_ = ((ExpressionComposer)ArithOps.MULTIPLY).compose(Arrays.asList(times, lit_8), Collections.<ExpressionType>nCopies(3, null));
        
        Expression arg2 = ((ExpressionComposer)ArithOps.ADD).compose(Arrays.asList(lit_9, lit_10), Collections.<ExpressionType>nCopies(3, null));
        
        // from unix exp
        Expression arg1 = FromUnixExpression.COMPOSER.compose(Arrays.asList(add, lit_varchar), Collections.<ExpressionType>nCopies(3, null));
        Expression arg1_ = FromUnixExpression.COMPOSER.compose(Arrays.asList(times_, lit_varchar), Collections.<ExpressionType>nCopies(3, null));
        
        // substr exp
        Expression substr = SubStringExpression.COMPOSER.compose(Arrays.asList(arg1, arg2, lit_11), Collections.<ExpressionType>nCopies(4, null));
        Expression substr_ = SubStringExpression.COMPOSER.compose(Arrays.asList(arg1_, arg2, lit_11), Collections.<ExpressionType>nCopies(4, null));
        
        ExplainContext context = new ExplainContext(); // Empty

        Explainer explainer = substr.getExplainer(context);
        String expResult = "SUBSTRING(FROM_UNIXTIME((123456 * 7) + 8, \'%Y-%m-%d\'), 9 + 10, 11)";
        DefaultFormatter f1 = new DefaultFormatter(null);
        String result = f1.format(explainer).get(0);
        assertEquals(expResult, result);
        
        DefaultFormatter f2 = new DefaultFormatter(null);
        explainer = substr_.getExplainer(context);
        expResult = "SUBSTRING(FROM_UNIXTIME(123456 * 7 * 8, \'%Y-%m-%d\'), 9 + 10, 11)";
        result = f2.format(explainer).get(0);
        assertEquals(expResult, result);
    }
}
