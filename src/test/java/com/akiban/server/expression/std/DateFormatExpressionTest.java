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

import java.util.TimeZone;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.ConverterTestUtils;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.*;


@RunWith(NamedParameterizedRunner.class)
public class DateFormatExpressionTest extends ComposedExpressionTestBase
{
    private String format;
    private String expected;
    private AkType argType;

    private static boolean alreadyExc = false;
    public DateFormatExpressionTest (AkType argType, String format, String expected)
    {
        this.format = format;
        this.expected = expected;
        this.argType = argType;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // DATE
        param(pb, AkType.DATE, "%y - %m - %d", "02 - 01 - 01");
        param(pb, AkType.DATE, "%Y-%M-%D %H %D%D", "2002-January-1st 00 1st1st"); // duplicate specifiers
        param(pb, AkType.DATE, "%U %V", "0 52");
        param(pb, AkType.DATE, "", null); // format string is empty => result is null
        param(pb, AkType.DATE, null, null); // format string is null => result is null
        param(pb, AkType.DATE, " %%Foo %%%%", " %Foo %%"); // format string only contains literal,
                                                           // , starts with % and ends with %
        param(pb, AkType.DATE, "% ", " ");
        param(pb, AkType.DATE, "% z %M", " z January"); // invalid specifier is treated as regular char
        param(pb, AkType.DATE, "%Y %%%", "2002 %%"); // % at the end is treated as literal %

        // DATETIME and TIMESTAMP
        for (AkType t : Arrays.asList(AkType.DATETIME, AkType.TIMESTAMP))
        {
            param(pb, t, "%y - %m - %d", "02 - 01 - 01");
            param(pb, t, "%Y-%M-%D %H", "2002-January-1st 11");
            param(pb, t, "%U %V", "0 52");
            param(pb, t, "", null);
            param(pb, t, null, null);
        }

        // TIME
        param(pb, AkType.TIME, "%H %i:%s", "11 30:56");
        param(pb, AkType.TIME, "", null);
        param(pb, AkType.TIME, null, null);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, AkType argType, String format, String expected)
    {
        pb.add("date_format( ," + argType + ", " + format + ") =" + expected, argType, format, expected);
    }

    @Test
    public void test()
    {
        // set time zone to UTC for testing
        // for the extractor
        String defaultTimeZone = TimeZone.getDefault().getID();
        ConverterTestUtils.setGlobalTimezone("EST");     

        Expression date = getExp(argType);
        Expression formatExp;
        if (format != null)
            formatExp = new LiteralExpression(AkType.VARCHAR, format);
        else
            formatExp = LiteralExpression.forNull();

        Expression top = DateFormatExpression.COMPOSER.compose(Arrays.asList(date, formatExp));
        check(top, expected);

        alreadyExc = true;
        
        // reset timezone
        ConverterTestUtils.setGlobalTimezone(defaultTimeZone);
    }

    private void check(Expression top, String expected)
    {
        if (expected == null)
            assertTrue ("top is null", top.evaluation().eval().isNull());
        else
            assertEquals(expected, top.evaluation().eval().getString());
    }

    private Expression getExp (AkType type)
    {
        switch (type)
        {
            case DATE:      return new LiteralExpression(AkType.DATE, extractLong("2002-01-01", AkType.DATE));
            case DATETIME:  return new LiteralExpression(AkType.DATETIME, extractLong("2002-01-01 11:30:56", AkType.DATETIME));
            case TIMESTAMP: return new LiteralExpression(AkType.TIMESTAMP, extractLong("2002-01-01 11:30:56", AkType.TIMESTAMP));
            case TIME:      return new LiteralExpression(AkType.TIME, extractLong("11:30:56", AkType.TIME));
            default:        return LiteralExpression.forNull();
        }
    }

    private long extractLong (String ex, AkType type)
    {
        return Extractors.getLongExtractor(type).getLong(ex);
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return DateFormatExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
