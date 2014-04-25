/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

@RunWith(NamedParameterizedRunner.class)
public final class DecimalParserTest {

    @TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        param(builder, "1234.56", 6, 2, "1234.56");
        param(builder, "-1234.56", 6, 2, "-1234.56");
        param(builder, "1234.56", 4, 2, "99.99");
        param(builder, "-1234.56", 4, 2, "-99.99");

        param(builder, "1234", 6, 2, "1234.00");
        param(builder, "12.3456", 4, 2, "12.35");
        param(builder, "1234.56", 6, 0, "1235");
        param(builder, "12.3Q", 4, 2, "12.30");

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder, 
                              String input, int precision, int scale, String expected) {
        builder.add(String.format("%s[%d,%d]", input, precision, scale),
                    input, precision, scale, expected);
    }

    public DecimalParserTest(String input, int precision, int scale, String expected) {
        this.input = input;
        this.precision = precision;
        this.scale = scale;
        this.expected = expected;
    }

    private String input, expected;
    private int precision, scale;

    @Test
    public void checkParse() {
        ValueSource source = new Value(MString.varcharFor(input), input);
        Value target = new Value(MNumeric.DECIMAL.instance(precision, scale, true));
        TExecutionContext context = 
            new TExecutionContext(null, Arrays.asList(source.getType()), target.getType(), null,
                                  ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE);
        MParsers.DECIMAL.parse(context, source, target);
        BigDecimal actual = ((BigDecimalWrapper)target.getObject()).asBigDecimal();
        assertEquals(input, new BigDecimal(expected), actual);
    }
}
