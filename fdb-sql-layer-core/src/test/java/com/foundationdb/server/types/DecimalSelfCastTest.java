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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.mcompat.MParsers;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

@RunWith(NamedParameterizedRunner.class)
public class DecimalSelfCastTest {

    @TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        param(builder, new BigDecimal("1234.56"), 6, 2, new BigDecimal("1234.56"));
        param(builder, new BigDecimal("-1234.56"), 6, 2, new BigDecimal("-1234.56"));
        param(builder, new BigDecimal("1234.56"), 4, 2, new BigDecimal("99.99"));
        param(builder, new BigDecimal("-1234.56"), 4, 2, new BigDecimal("-99.99"));

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder,
                        BigDecimal source, int precision, int scale, 
                        BigDecimal expected)  {
        builder.add(String.format("%s[%d,%d]", source.toString(), precision, scale),
                source, precision, scale, expected);
    }

    public DecimalSelfCastTest(BigDecimal source, int precision, int scale, BigDecimal expected) { 
        this.source = source;
        this.precision = precision;
        this.scale = scale;
        this.expected = expected;
    }

    private BigDecimal source, expected;
    private int precision, scale;

    @Test
    public void checkSelfCast() {
        Value start = new Value (MNumeric.DECIMAL.instance(source.precision(), source.scale(), false));
        start.putObject(new BigDecimalWrapperImpl(source));
        Value target = new Value (MNumeric.DECIMAL.instance(precision, scale, false));
        

        TExecutionContext context = 
                new TExecutionContext(null, Arrays.asList(start.getType()), target.getType(), null,
                                      ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE);
        target.getType().typeClass().selfCast(context, start.getType(), start, target.getType(), target);
        
        
        BigDecimal actual = ((BigDecimalWrapper)target.getObject()).asBigDecimal();
        
        
        assertEquals(expected, actual);
    }


}
