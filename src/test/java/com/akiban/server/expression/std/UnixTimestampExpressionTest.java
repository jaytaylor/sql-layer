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

import org.junit.Before;
import java.text.ParseException;
import org.joda.time.DateTime;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.ConverterTestUtils;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.*;

public class UnixTimestampExpressionTest extends ComposedExpressionTestBase
{
    @Before
    public void setup()
    {
        ConverterTestUtils.setGlobalTimezone("UTC");
    }
    
    @Test
    public void testOneArg() throws ParseException
    {
        
        String ts = "2012-05-21 12:34:10";
        DateTime expected = new DateTime(2012,5,21, 12, 34, 10,0, DateTimeZone.UTC);
 
        Expression arg = new LiteralExpression(AkType.TIMESTAMP, 
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong(ts));
       
        Expression top = new UnixTimestampExpression(Arrays.asList(arg));
        assertEquals(expected.getMillis() / 1000, top.evaluation().eval().getLong());
    }
    
    @Test
    public void testOneArgEpoch()
    {
        Expression arg = new LiteralExpression(AkType.TIMESTAMP, 
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("1970-01-01 00:00:00"));
        Expression top = new UnixTimestampExpression(Arrays.asList(arg));
        assertEquals(0L, top.evaluation().eval().getLong());
    }
    
    @Test
    public void testoneArgBeforeEpoch()
    {
        Expression arg = new LiteralExpression(AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("1920-01-01 00:00:00"));
        Expression top = new UnixTimestampExpression(Arrays.asList(arg));
        assertEquals(0L, top.evaluation().eval().getLong());
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.TIMESTAMP, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
         return UnixTimestampExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
}
