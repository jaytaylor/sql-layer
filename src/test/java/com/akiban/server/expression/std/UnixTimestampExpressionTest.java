
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
