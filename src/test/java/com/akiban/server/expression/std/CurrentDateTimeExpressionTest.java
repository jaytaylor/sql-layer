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

import java.util.Date;
import java.text.SimpleDateFormat;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.AkType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CurrentDateTimeExpressionTest 
{
    @Test
    public void testCurrentDateinString()
    {
        test(true, AkType.DATE, "yyyy-MM-dd");
    }

    @Test
    public void testCurrentDateInNumeric()
    {
        test(false, AkType.DATE, "yyyyMMdd");
    }

    @Test
    public void testCurrentTimeInString()
    {
        test(true,AkType.TIME, "hh:mm:ss");
    }

    @Test
    public void testCurrentTimeInNumeric()
    {
        test(false, AkType.TIME, "hhmmss");
    }

    @Test
    public void testCurrentTimestampInString()
    {
        test(true, AkType.TIMESTAMP, "yyyy-MM-dd hh:mm:ss");
    }

    @Test
    public void testCurrentTimestampInNumeric()
    {
        test(false, AkType.TIMESTAMP, "yyyyMMddhhmmss");
    }

    private void test(boolean isString, AkType type, String format)
    {
        ValueSource actual = new CurrentDateTimeExpression(isString, type).evaluation().eval();
        ValueSource expected;
        if (isString)
            expected = new ValueHolder(AkType.VARCHAR, new SimpleDateFormat(format).format(new Date()));
        else
            expected = new ValueHolder(AkType.LONG, Long.parseLong(new SimpleDateFormat(format).format(new Date())));
        assertEquals(actual, expected);
    }
}
