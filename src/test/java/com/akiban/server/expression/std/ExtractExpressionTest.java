/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
package com.akiban.server.expression.std;

import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExtractExpressionTest extends ComposedExpressionTestBase
{
    private static final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.DATE, false);

    // --------------------------- GET DATE-------------------------------------
    @Test
    public void getDateFromDate()
    {
        Expression arg = new LiteralExpression(AkType.DATE, 1234L);
        Expression top = ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));

        assertEquals(1234L, top.evaluation().eval().getDate());
    }

    @Test
    public void getDateFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, getDatetime());

        assertEquals("2009-08-30", Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }

    @Test
    public void getDateFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, getTimestamp());
        assertEquals("2009-08-30", Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }

    @Test
    public void getDateFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, arg);

        assertEquals("2009-12-30", Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }

    @Test
    public void getDateFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDateFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, arg);

        assertEquals("2009-12-30", Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));

    }

    @Test
    public void getDateFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDateFromYear()
    {
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDateFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }

    //------------------------GET DATETIME--------------------------------------
    @Test
    public void getDatetimeFromDate()
    {
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, getDate());

        long s = top.evaluation().eval().getDateTime();
        
        assertEquals(20090830000000L, s);
    }

    @Test
    public void getDatetimeFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, getDatetime());

        assertEquals(20090830113045L, top.evaluation().eval().getDateTime());
    }

    @Test
    public void getDatetimeFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, getTimestamp());
        assertEquals(20090830113045L, top.evaluation().eval().getDateTime());
    }
                

    @Test
    public void getDatetimeFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, arg);

        assertEquals("2009-12-30 12:30:10", Extractors.getLongExtractor(AkType.DATETIME).asString(top.evaluation().eval().getDateTime()));
    }

    @Test
    public void getDatetimeFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDatetimeFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, arg);

        assertEquals(20091230123045L,top.evaluation().eval().getDateTime());

    }

    @Test
    public void getDateTimeFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDatetimeFromYear()
    {
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDatetimeFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.DATETIME_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    //----------------------- GET DAY-------------------------------------------
    @Test
    public void getDayFromDate()
    {
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, getDate());
        assertEquals(30L, top.evaluation().eval().getLong());

    }

    @Test
    public void getDayFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, getDatetime());
        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getDayFromTime()
    {
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, getTime());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDayFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, getTimestamp());
        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getDayFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, arg);

        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getDayFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDayFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, arg);

        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getDayFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDayFromYear()
    {
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getDayFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }

    // ------------------------ GET HOUR (or HOUR_OF_DAY)-----------------------
    @Test
    public void getHourFromDate()
    {
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, getDate());
        assertTrue(top.evaluation().eval().isNull());

    }

    @Test
    public void getHourFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, getDatetime());
        assertEquals(11L, top.evaluation().eval().getLong());
    }

    @Test
    public void getHourFromTime()
    {
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, getTime());
        assertEquals(11L, top.evaluation().eval().getLong());
    }

    @Test
    public void getHourFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, getTimestamp());
        assertEquals(11L, top.evaluation().eval().getLong());
    }

    @Test
    public void getHourFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, arg);

        assertEquals(12L, top.evaluation().eval().getLong());
    }

    @Test
    public void getHourFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getHourFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, arg);

        assertEquals(12L, top.evaluation().eval().getLong());
    }

    @Test
    public void getHourFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getHourFromYear()
    {
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getHourFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.HOUR_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    //-----------------------------GET MINUTE-----------------------------------
    @Test
    public void getMinuteFromDate()
    {
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, getDate());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMinuteFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, getDatetime());
        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMinuteFromTime()
    {
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, getTime());
        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMinuteFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, getTimestamp());
        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMinuteFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, arg);

        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMinuteFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMinuteFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, arg);

        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMinuteFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMinuteFromYear()
    {
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMinuteFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.MINUTE_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    // ----------------------- GET MONTH----------------------------------------
    @Test
    public void getMonthFromDate()
    {
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, getDate());
        assertEquals(8L, top.evaluation().eval().getLong());

    }

    @Test
    public void getMonthFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, getDatetime());
        assertEquals(8L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMonthFromTime()
    {
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, getTime());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMonthFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, getTimestamp());
        assertEquals(8L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMonthFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-08-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, arg);

        assertEquals(8L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMonthFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMonthFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, arg);

        assertEquals(30L, top.evaluation().eval().getLong());
    }

    @Test
    public void getMonthFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMonthFromYear()
    {
        Expression top = getTopExp(ExtractExpression.DATE_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getMonthFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.MONTH_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    // ----------------------- GET SECOND---------------------------------------
    @Test
    public void getSecondFromDate()
    {
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, getDate());
        assertTrue(top.evaluation().eval().isNull());

    }

    @Test
    public void getSecondFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, getDatetime());
        assertEquals(45L, top.evaluation().eval().getLong());
    }

    @Test
    public void getSecondFromTime()
    {
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, getTime());
        assertEquals(45L, top.evaluation().eval().getLong());
    }

    @Test
    public void getSecondFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, getTimestamp());
        assertEquals(45L, top.evaluation().eval().getLong());
    }

    @Test
    public void getSecondFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, arg);

        assertEquals(10L, top.evaluation().eval().getLong());
    }

    @Test
    public void getSecondFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getSecondFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, arg);

        assertEquals(45L, top.evaluation().eval().getLong());
    }

    @Test
    public void getSecondFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getSecondFromYear()
    {
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }


    @Test
    public void getSecondFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.SECOND_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    // -------------------------GET TIME----------------------------------------

    @Test
    public void getTimeFromTime()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, new LiteralExpression(AkType.TIME, 1234L));
        assertEquals(1234L, top.evaluation().eval().getTime());
    }

    @Test
    public void getTimeFromDate()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, getDate());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimeFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, getDatetime());
        assertEquals("11:30:45", Extractors.getLongExtractor(AkType.TIME).asString(top.evaluation().eval().getTime()));
    }

    @Test
    public void getTimeFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, getTimestamp());
        assertEquals("11:30:45", Extractors.getLongExtractor(AkType.TIME).asString(top.evaluation().eval().getTime()));
    }

    @Test
    public void getTimeFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, arg);

        assertEquals("12:30:10", Extractors.getLongExtractor(AkType.TIME).asString(top.evaluation().eval().getTime()));
    }

    @Test
    public void getTimeFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimeFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, arg);

        assertEquals("12:30:45", Extractors.getLongExtractor(AkType.TIME).asString(top.evaluation().eval().getTime()));

    }

    @Test
    public void getTimeFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimeFromYear()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimeFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    // ----------------------- GET TIMESTAMP------------------------------------
    @Test
    public void getTimestampFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, new LiteralExpression(AkType.TIMESTAMP, 1234L));
        assertEquals(1234L, top.evaluation().eval().getTimestamp());
    }

    @Test
    public void getTimestampFromDate()
    {
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, getDate());
        assertEquals("2009-08-30 00:00:00", Extractors.getLongExtractor(AkType.TIMESTAMP).asString(top.evaluation().eval().getTimestamp()));
   }

    @Test
    public void getTimestampFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, getDatetime());
        assertEquals("2009-08-30 11:30:45", Extractors.getLongExtractor(AkType.TIMESTAMP).asString(top.evaluation().eval().getTimestamp()));
    }

    @Test
    public void getTimestampFromTime()
    {
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, getTime());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimestampFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, arg);

        assertEquals("2009-12-30 12:30:10", Extractors.getLongExtractor(AkType.TIMESTAMP).asString(top.evaluation().eval().getTimestamp()));
    }

    @Test
    public void getTimestampFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimestampFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, arg);

        assertEquals("2009-12-30 12:30:45", Extractors.getLongExtractor(AkType.TIMESTAMP).asString(top.evaluation().eval().getTimestamp()));

    }

    @Test
    public void getTimestampFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getTimestampFromYear()
    {
        Expression top = getTopExp(ExtractExpression.TIME_COMPOSER, getYear());
        assertTrue(top.evaluation().eval().isNull());
    }


    @Test
    public void getTimestampFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.TIMESTAMP_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
    // ------------------------GET YEAR-----------------------------------------
    @Test
    public void getYearFromTimestamp()
    {
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, getTimestamp());
        assertEquals(2009L, top.evaluation().eval().getYear());
    }

    @Test
    public void getYearFromDate()
    {
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, getDate());
        assertEquals(2009L, top.evaluation().eval().getYear());
   }

    @Test
    public void getYearstampFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, getDatetime());
        assertEquals(2009L, top.evaluation().eval().getYear());
    }

    @Test
    public void getYearFromTime()
    {
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, getTime());
        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getYearFromVarchar()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, arg);

        assertEquals(2009L, top.evaluation().eval().getYear());
    }

    @Test
    public void getYearFromBadString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getYearFromLong()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, arg);

        assertEquals(2009L, top.evaluation().eval().getYear());

    }

    @Test
    public void getYearFromDouble()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, arg);

        assertTrue(top.evaluation().eval().isNull());
    }

    @Test
    public void getYearFromYear()
    {
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, getYear());
        assertEquals ("2009", Extractors.getLongExtractor(AkType.YEAR).asString(top.evaluation().eval().getYear()));
    }

    @Test
    public void getYearFromNull ()
    {
        Expression top = getTopExp(ExtractExpression.YEAR_COMPOSER, LiteralExpression.forNull());
        assertTrue (top.evaluation().eval().isNull());
    }
     
    //--------------------------------------------------------------------------

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ExtractExpression.DATE_COMPOSER;
    }

    private Expression getDatetime()
    {
        return new LiteralExpression(AkType.DATETIME, 20090830113045L);
    }

    private Expression getDate()
    {
        return new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2009-08-30"));
    }

    private Expression getTime()
    {
        return new LiteralExpression(AkType.TIME, Extractors.getLongExtractor(AkType.TIME).getLong("11:30:45"));
    }

    private Expression getTimestamp()
    {
        return new LiteralExpression(AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-08-30 11:30:45"));
    }

    private Expression getYear()
    {
        return new LiteralExpression(AkType.YEAR, 109L);
    }

    private Expression getTopExp(ExpressionComposer composer, Expression arg)
    {
        return composer.compose(Arrays.asList(arg));
    }
}
