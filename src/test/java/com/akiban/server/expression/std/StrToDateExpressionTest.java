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

        param(pb, "Test date to date, std delimeter", "2009-12-28", "%Y-%m-%d", "2009-12-28", AkType.DATE);
        param(pb, "Test date to date, delimeter is [abc]", "2009abc12abc28", "%Yabc%mabc%d", "2009-12-28", AkType.DATE);
        param(pb, "Test date to date, using different delimeters", "2009~12/28", "%Y~%m/%d", "2009-12-28", AkType.DATE);
        param(pb, "Test date to date, no delimeters", "090812", "%y%d%m", "2009-12-08", AkType.DATE);
        param(pb, "Test year to date", "2009", "%Y", "2009-00-00", AkType.DATE);
        param(pb, "Test year-month to date", "09Mar", "%y%b", "2009-03-00", AkType.DATE);
        param(pb, "Test year-mont-hour to datetime", "09 Mar13", "%y %b%H", "2009-03-00 13:00:00", AkType.DATETIME);
        param(pb, "Test datetime to datetime, different dels", "09~23Mar=01455", "%y~%H%b=%d%i%s", "2009-03-01 23:45:05", AkType.DATETIME);
        param(pb, "Test mis-matched str and format, expected null", "2009-12-30", "%Y/%m/%d", "", AkType.DATE);
        param(pb, "Test hour-minute to time", "9-18", "%h-%i", "09:18:00", AkType.TIME);
        param(pb, "Test time to time", "09569", "%H%i%s", "09:56:09", AkType.TIME);
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
