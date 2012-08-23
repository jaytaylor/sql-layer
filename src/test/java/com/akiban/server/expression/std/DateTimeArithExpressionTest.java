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

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;

public class DateTimeArithExpressionTest extends ComposedExpressionTestBase
{
    LongExtractor extractor = Extractors.getLongExtractor(AkType.DATE);
    
    @Test
    public void testDateAdd ()
    {       
        Expression left1 = new LiteralExpression(AkType.DATE, extractor.getLong("2009-12-12"));
        Expression right1 = new LiteralExpression(AkType.LONG, 12L); // second arg is a LONG
        
        Expression left2 = new LiteralExpression(AkType.DATE, extractor.getLong("2009-12-24"));
        
        // top1 is a DATE: ADDDATE('2009-12-12', 12)
        Expression top1 = compose(DateTimeArithExpression.ADD_DATE_COMPOSER, Arrays.asList(left1, right1));
        
        // top2 is an interval
        Expression top2 = compose(ArithOps.MINUS, Arrays.asList(left2, left1));
        
        // top3 is a DATE: ADDDATE("2009-12-12", interval 12 days)
        Expression top3 = compose(DateTimeArithExpression.ADD_DATE_COMPOSER, Arrays.asList(left1, top2));
        
        assertEquals("assert top1 == 2009-12-24", extractor.getLong("2009-12-24"),top1.evaluation().eval().getDate());
        assertEquals("assert top3 == top1", top1.evaluation().eval().getDate(), top3.evaluation().eval().getDate());
    }
    
    @Test
    public void testDateAdd_IntervalMonth ()
    {   
        Expression left = new LiteralExpression(AkType.DATE, extractor.getLong("2008-02-29"));
        Expression right = new LiteralExpression(AkType.INTERVAL_MONTH, 12L);
        
        Expression top = compose(DateTimeArithExpression.ADD_DATE_COMPOSER, Arrays.asList(left, right));
        assertEquals(extractor.getLong("2009-02-28"), top.evaluation().eval().getDate());
    }
    
    @Test
    public void testTimeDiff ()
    {
        Expression l = new LiteralExpression(AkType.TIME,100915L);
        Expression r = new LiteralExpression(AkType.TIME, 90915L);

        Expression top = compose(DateTimeArithExpression.TIMEDIFF_COMPOSER, Arrays.asList(l,r));
        long actual = top.evaluation().eval().getTime();

        assertEquals(10000L, actual);
    }

    @Test
    public void testDateTimeDiff()
    {
        Expression l = new LiteralExpression(AkType.DATETIME, 20091010123010L);
        Expression r = new LiteralExpression(AkType.DATETIME, 20091010123001L);

        Expression top = compose(DateTimeArithExpression.TIMEDIFF_COMPOSER, Arrays.asList(r,l));
        long actual = top.evaluation().eval().getTime();

        assertEquals(-9L, actual);
    }

    @Test
    public void testTimeStampDiff ()
    {
        Expression l = new LiteralExpression(AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-12-10 10:10:10"));
        Expression r = new LiteralExpression(AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-12-11 10:09:09"));

        Expression top = compose(DateTimeArithExpression.TIMEDIFF_COMPOSER, Arrays.asList(l,r));
        long actual = top.evaluation().eval().getTime();

        assertEquals(-235859L, actual);
    }

    @Test
    public void testNullTimeDiff ()
    {
        Expression l = LiteralExpression.forNull();
        Expression r = new LiteralExpression(AkType.TIME, 1234L);

        Expression top = compose(DateTimeArithExpression.TIMEDIFF_COMPOSER, Arrays.asList(l,r));
        assertTrue ("top is null", top.evaluation().eval().isNull());
    }

    @Test
    public void testDateDiff ()
    {
        test("2012-08-14", "2010-01-01", 956);
        test("2010-01-01", "2012-08-14", -956);
        test("2011-12-05", "2011-11-01", 34L);
        test("2009-12-10", "2010-01-12", -33L);
        test("2008-02-01", "2008-03-01", -29L);
        test("2010-01-01", "2010-01-01", 0L);
    }

    @Test
    public void testNullDateiff()
    {
        Expression l = LiteralExpression.forNull();
        Expression r = new LiteralExpression(AkType.DATE, 1234L);

        Expression top = compose(DateTimeArithExpression.DATEDIFF_COMPOSER, Arrays.asList(l, r));
        assertTrue("top is null", top.evaluation().eval().isNull());
    }

    @Test()
    public void testInvalidArgumenTypeDateDiff ()
    {
        Expression l = new LiteralExpression(AkType.DATE, 1234L);
        Expression r = new LiteralExpression(AkType.DATETIME, 1234L);

        Expression top = compose(DateTimeArithExpression.DATEDIFF_COMPOSER, Arrays.asList(l, r));
        assertTrue("Top should be NULL ", top.evaluation().eval().isNull());
    }

    @Test()
    public void testInvalidArgumenTypeTimeDiff ()
    {
        Expression l = new LiteralExpression(AkType.DATE, 1234L);
        Expression r = new LiteralExpression(AkType.DATETIME, 1234L);

        Expression top = compose(DateTimeArithExpression.TIMEDIFF_COMPOSER, Arrays.asList(l, r));
        assertTrue("Top should be NULL ", top.evaluation().eval().isNull());
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.DATE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return DateTimeArithExpression.DATEDIFF_COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
    // ---------------- private method -----------------
        
    private static void test (String left, String right, long expected)
    {
        LongExtractor ex = Extractors.getLongExtractor(AkType.DATE);
        Expression l = new LiteralExpression(AkType.DATE,ex.getLong(left));
        Expression r = new LiteralExpression(AkType.DATE, ex.getLong(right));

        Expression top = compose(DateTimeArithExpression.DATEDIFF_COMPOSER, Arrays.asList(l,r));
        long actual = top.evaluation().eval().getLong();

        assertEquals(expected, actual);
    }
}
