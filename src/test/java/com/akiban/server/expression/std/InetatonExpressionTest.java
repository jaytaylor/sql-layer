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

import com.akiban.server.types.AkType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InetatonExpressionTest  extends ComposedExpressionTestBase
{
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.LONG, true);

    @Test
    public void test4Quads ()
    {
        test("255.0.9.5", 4278192389L);
    }

     @Test
    public void test3Quads ()
    {
        test("255.1.1", 4278255617L); // equivalent to 255.1.0.1
    }

    @Test
    public void test2Quads ()
    {
        test("127.1", 2130706433); // equivalent to 127.0.0.1
    }

    @Test
    public void test1Quad()
    {
        test("127", 127); // equivalent to 0.0.0.127
    }

    @Test
    public void testZeroQuad()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, ""));
    }

    @Test
    public void test5Quads() // do not accept IPv6 or anything other than Ipv4
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR,"1.2.3.4.5"));
    }

    @Test
    public void testNull ()
    {
        testExpectNull(new LiteralExpression(AkType.NULL, null));
    }

    @Test
    public void testBadFormatString ()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "12sdfa"));
    }

    @Test
    public void testNonNumeric ()
    {        
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "a.b.c.d"));
    }

    @Test
    public void testNeg ()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "-127.0.0.1"));
    }

    @Test
    public void testNumberOutofRange ()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "256.0.1.0"));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return InetatonExpression.COMPOSER;
    }

    //------------ private methods-------------------------
    private void testExpectNull (Expression arg)
    {
        assertTrue((new InetatonExpression(arg)).evaluation().eval().isNull());
    }
    private void test (String ip, long expected)
    {
        assertEquals(expected,
                (new InetatonExpression(new LiteralExpression(AkType.VARCHAR, ip))).evaluation().eval().getLong());
    }

    @Override
    public boolean alreadyExc()
    {
        return false;
    }
}
