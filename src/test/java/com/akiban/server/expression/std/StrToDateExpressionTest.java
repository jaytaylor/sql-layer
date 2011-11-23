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

import com.akiban.server.types.extract.LongExtractor;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.AkType;
import com.akiban.server.expression.Expression;
import org.junit.Test;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class StrToDateExpressionTest 
{
    private String str;
    private String format;
    private String expected;
    private AkType type;

    public StrToDateExpressionTest (String str, String format, String expected, AkType type)
    {
        this.str = str;
        this.format = format;
        this.expected = expected;
        this.type = type;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // DATE
        param(pb, "date to date, std delimeter", "2009-12-28", "%Y-%m-%d", "2009-12-28", AkType.DATE);
        param(pb, "date to date, delimeter is [abc]", "2009abc12abc28", "%Yabc%mabc%d", "2009-12-28", AkType.DATE);
        param(pb, "date to date, using different delimeters", "2009~12/28", "%Y~%m/%d", "2009-12-28", AkType.DATE);
        param(pb, "date to date, no delimeters", "090812", "%y%d%m", "2009-12-08", AkType.DATE);
        param(pb, "year to date", "2009", "%Y", "2009-00-00", AkType.DATE);
        param(pb, "year-month to date", "09Mar", "%y%b", "2009-03-00", AkType.DATE);
        param(pb, "month-year-day", "November0912", "%M%y%d", "2009-11-12", AkType.DATE);
        param(pb, "datetime to datetime, different dels", "09~23Mar=01455", "%y~%H%b=%d%i%s", "2009-03-01 23:45:05", AkType.DATETIME);
        param(pb, "mis-matched str and format, expected null", "2009-12-30", "%Y/%m/%d", "", AkType.DATE);
        param(pb, "unparsable string", "abcd1208", "%Y%m%d", "", AkType.DATE);
        param(pb, "duplicate fields, expecting null", "20090910", "%Y%y%m", "", AkType.DATE);
        param(pb, "week - year - weekday to date", "200442 Monday", "%X%V %W", "2004-10-18", AkType.DATE);
        param(pb, "week year with %x%v", "200442 Mon", "%x%v %a", "2004-10-11", AkType.DATE);
        param(pb, "date with leading strings", "at date 09-10-13", "at date %y-%m-%d", "2009-10-13", AkType.DATE);
        param(pb, "date with trailing strings", "2009-10-14at date", "%Y-%m-%d at date", "2009-10-14", AkType.DATE);
        
        // TIME
        param(pb, "hour-minute to time", "9-18", "%h-%i", "09:18:00", AkType.TIME);
        param(pb, "hour-minute-second to time", "23~34_3", "%k~%i_%s", "23:34:03", AkType.TIME);
        param(pb, "illegal value for 12-hr-format hour, expect null", "13:10:20", "%h:%i:%s", "", AkType.TIME);
        param(pb, "legal value for 24-hr format hour", "13:10:20", "%H:%i:%s", "13:10:20", AkType.TIME);
        param(pb, "minute-sec to time", "09    18", "%i %s", "00:09:18", AkType.TIME);
        param(pb, "time to time", "09569", "%H%i%s", "09:56:09", AkType.TIME);
        param(pb, "time with duplicate hour fields to time, expect null", "12:20:10:13", "%H:%i:%h:%s", "", AkType.TIME);
        param(pb, "%r to time", "11:30:10", "%r", "11:30:10", AkType.TIME);
        param(pb, "%r and time to time" , "12:30:40 11:25:35", "%r %H:%i:%s", "00:30:40", AkType.TIME); // %H:%i:%s part is simply ignored
        param(pb, "time with AM/PM", "12:30:10 am", "%r %p", "00:30:10", AkType.TIME);
        param(pb, "%r time with AM/PM", "12:30:10 pm", "%r %p", "12:30:10", AkType.TIME);
        param(pb, "12hr time wiht AM/PM", "8:20:10 am", "%h:%i:%s %p", "08:20:10", AkType.TIME);
        param(pb, "24hr time with AM/PM, expect null", "0:20:10 PM", "%H:%i:%s %p", "", AkType.TIME);
        param(pb, "24hr time with AM/PM, expect null", "13:20:10 PM", "%T %p", "", AkType.TIME);
        
        //DATETIME
        param(pb, "year-month-hour to datetime", "09 Mar13", "%y %b%H", "2009-03-00 13:00:00", AkType.DATETIME);
        param(pb, "%r - year-month-day to datetime", "00:30:10 2006~07Nov", "%r %Y~%d%b", "2006-11-07 00:30:10", AkType.DATETIME);
        param(pb, "%T - week - day to datetime", "12:30:40201203 Tuesday", "%T%X%V %W", "2012-01-17 12:30:40", AkType.DATETIME);
        param(pb, "%r %p week -day %x%v to datetime", "06:23:30 pm 2004 04 Mon", "%r %p %x %v %a", "2004-01-19 18:23:30", AkType.DATETIME);
       
        // unknown specifier
        param(pb, "unknown specifier %z, expect null", "2009-01-01", "%Y-%z-%d", "", AkType.NULL);
  
        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, String testName, String str, String format, String expected, AkType type)
    {
        pb.add(testName + ": str_to_date(" + str + ", " + format + ") = " + expected, str, format, expected, type);
    }

    @Test
    public void test ()
    {
        Expression strExp = new LiteralExpression(AkType.VARCHAR, str);
        Expression fmExp = new LiteralExpression (AkType.VARCHAR, format);
        Expression top = new StrToDateExpression(strExp, fmExp);

        assertTrue(top.valueType() == type);
        if (expected.equals(""))
            assertTrue(top.evaluation().eval().isNull());
        else
        {
            LongExtractor extractor = Extractors.getLongExtractor(type);
            assertEquals(expected, extractor.asString(extractor.getLong(top.evaluation().eval())));
        }
    }
}
