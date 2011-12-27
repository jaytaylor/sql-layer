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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
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

        ConverterTestUtils.setGlobalTimezone("UTC");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        
        LongExtractor dateExtractor = Extractors.getLongExtractor(AkType.DATE);
        LongExtractor tsExtractor = Extractors.getLongExtractor(AkType.TIMESTAMP);

        // to DATETIME
        value = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        expected = new ValueHolder(AkType.DATETIME, 20061007000000L);
        assertEquals(expected, cast(value, AkType.DATETIME));
        
        value = new ValueHolder(AkType.TIMESTAMP, tsExtractor.getLong("2006-10-07 15:30:10"));
        expected = new ValueHolder(AkType.DATETIME, 20061007153010L);
        assertEquals(expected, cast(value, AkType.DATETIME));

        // to DATE
        value = new ValueHolder(AkType.DATETIME, 20061007123010L);
        expected = new ValueHolder(AkType.DATE, dateExtractor.getLong("2006-10-07"));
        assertEquals(expected, cast(value, AkType.DATE));
    }



}
