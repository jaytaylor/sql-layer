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

import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class ConvertTZExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    private static final LongExtractor extractor = Extractors.getLongExtractor(AkType.DATETIME);

    private final String dt;
    private final String from;
    private final String to;
    
    private final Long expected;
    public ConvertTZExpressionTest (String dt, String from, String to, Long exp)
    {
        this.dt = dt;
        this.from = from;
        this.to = to;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder b = new ParameterizationBuilder();
        
        param(b, "2009-12-12 00:00:00", "-06:00", "+5:00", 20091212110000L);
        param(b, "2004-01-01 12:00:00", "GMT", "MET", 20040101130000L);
        param(b, "2003-06-01 12:59:59", "-1:00", "+10:00", 20030601235959L);

        return b.asList();
    }
    
    private static void param(ParameterizationBuilder bd, String dt, String from, String to, Long exp)
    {
        bd.add("CONVET_TZ(" + dt + ", " + from + ", " + to + ")", dt, from, to, exp);
    }

    @Test
    public void test()
    {
        alreadyExc = true;
        Expression date = dt == null
                          ? LiteralExpression.forNull()
                          : new LiteralExpression(AkType.DATETIME, 
                                                 extractor.getLong(dt));
        
        Expression frm = from == null
                          ? LiteralExpression.forNull()
                          : new LiteralExpression(AkType.VARCHAR, from);
        
        Expression t = to == null
                       ? LiteralExpression.forNull()
                       : new LiteralExpression(AkType.VARCHAR, to);
        
        ValueSource top = new ConvertTZExpression(Arrays.asList(date, frm, t)).evaluation().eval();
        
        if (expected == null)
            assertTrue("Top should be NULL ", top.isNull());
        else
            assertEquals(extractor.asString(expected.longValue()), 
                         extractor.asString(top.getDateTime()));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.DATETIME, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ConvertTZExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
