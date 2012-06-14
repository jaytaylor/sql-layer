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

import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.ConverterTestUtils;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.TimeZone;

public final class CastExpressionTest 
{
    protected ValueSource cast(ValueSource source, AkType to) {
        Expression expression = new CastExpression(to, new LiteralExpression(source));
        return expression.evaluation().eval();
    }

    @Test
    public void testNull() {
        ValueSource booleanNull = BoolValueSource.OF_NULL;
        assertTrue("isNull but not AkType.NULL",
                   (booleanNull.isNull() &&
                    (booleanNull.getConversionType() != AkType.NULL)));
        ValueSource result = cast(booleanNull, AkType.VARCHAR);
        assertTrue("result is null", result.isNull());
        assertTrue("result is NULL", (result.getConversionType() == AkType.NULL));
    }

    @Test
    public void testSame() {
        ValueSource value;

        value = new ValueHolder(AkType.INT, 1);
        assertEquals(value, cast(value, AkType.INT));

        value = new ValueHolder(AkType.VARCHAR, "test");
        assertEquals(value, cast(value, AkType.VARCHAR));
    }

    @Test
    public void testConvert() {
        ValueSource value, expected;

        value = new ValueHolder(AkType.INT, 20);
        expected = new ValueHolder(AkType.VARCHAR, "20");
        assertEquals(expected, cast(value, AkType.VARCHAR));

        value = new ValueHolder(AkType.VARCHAR, "-123");
        expected = new ValueHolder(AkType.LONG, -123L);
        assertEquals(expected, cast(value, AkType.LONG));

        value = new ValueHolder(AkType.VARCHAR, "98.76");
        expected = new ValueHolder(AkType.DECIMAL, new BigDecimal("98.76"));
        assertEquals(expected, cast(value, AkType.DECIMAL));

        String defaultTimeZone = TimeZone.getDefault().getID();
        ConverterTestUtils.setGlobalTimezone("EST");     
        
        LongExtractor dateExtractor = Extractors.getLongExtractor(AkType.DATE);
        LongExtractor tsExtractor = Extractors.getLongExtractor(AkType.TIMESTAMP);

        // to DATETIME
        value = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        expected = new ValueHolder(AkType.DATETIME, 20061007000000L);
        assertEquals(expected, cast(value, AkType.DATETIME));
        
        value = new ValueHolder(AkType.TIMESTAMP, tsExtractor.getLong("2006-10-07 15:30:10"));
        expected = new ValueHolder(AkType.DATETIME, 20061007153010L);
        assertEquals(expected, cast(value, AkType.DATETIME));

        // to TIMESTAMP
        value = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        expected = new ValueHolder(AkType.TIMESTAMP, tsExtractor.getLong("2006-10-07 00:00:00"));
        assertEquals(expected, cast(value, AkType.TIMESTAMP));

        value = new ValueHolder(AkType.DATETIME, 20061007123010L);
        expected = new ValueHolder(AkType.TIMESTAMP, tsExtractor.getLong("2006-10-07 12:30:10"));
        assertEquals(expected, cast(value, AkType.TIMESTAMP));

        // to DATE
        value = new ValueHolder(AkType.DATETIME, 20061007123010L);
        expected = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        assertEquals(expected, cast(value, AkType.DATE));

        value = new ValueHolder(AkType.TIMESTAMP, tsExtractor.getLong("2006-10-07 12:30:10"));
        assertEquals(expected, cast(value, AkType.DATE));

        // to YEAR
        value = new ValueHolder(AkType.DATETIME, 20061007123010L);
        expected = new ValueHolder(AkType.YEAR, 2006L - 1900L);
        assertEquals(expected, cast(value, AkType.YEAR));

        value = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        assertEquals(expected, cast(value,AkType.YEAR));

        value = new ValueHolder(AkType.TIMESTAMP, tsExtractor.getLong("2006-10-07 12:30:10"));
        assertEquals(expected, cast(value,AkType.YEAR));

        // to TIME
        expected = new ValueHolder(AkType.TIME, 123010L);
        assertEquals(expected, cast(value, AkType.TIME));

        value = new ValueHolder(AkType.DATETIME, 20061007123010L);
        assertEquals(expected, cast(value, AkType.TIME));

        value = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        expected = new ValueHolder(AkType.TIME, 0L);
        assertEquals(expected, cast(value, AkType.TIME));

        value = new ValueHolder(AkType.YEAR, 2006L - 1900L);
        assertEquals(expected, cast(value, AkType.TIME));

        // varchar to date
        value = new ValueHolder(AkType.VARCHAR, "2006-10-06 12:30:10");
        expected = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-06"));
        assertEquals(expected, cast(value, AkType.DATE));
        
        // reset timezone
        ConverterTestUtils.setGlobalTimezone(defaultTimeZone);
    }
}
