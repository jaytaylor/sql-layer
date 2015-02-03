/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.Value;
import com.google.common.primitives.UnsignedLongs;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

@RunWith(SelectedParameterizedRunner.class)
public class MTypesComparisonTest extends TypeComparisonTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        return makeParams(
            MBundle.INSTANCE.id(),
            Arrays.asList(
                typeInfo(MApproximateNumber.FLOAT, -Float.MAX_VALUE, 0f, Float.MAX_VALUE),
                typeInfo(MApproximateNumber.FLOAT_UNSIGNED, 0f, Float.MAX_VALUE),
                typeInfo(MApproximateNumber.DOUBLE, -Double.MAX_VALUE, 0d, Double.MAX_VALUE),
                typeInfo(MApproximateNumber.DOUBLE_UNSIGNED, 0d, Double.MAX_VALUE),

                typeInfo(MNumeric.TINYINT, -128, 0, 127),
                typeInfo(MNumeric.TINYINT_UNSIGNED, 0, 255),
                typeInfo(MNumeric.SMALLINT, -32768, 0, 32767),
                typeInfo(MNumeric.SMALLINT_UNSIGNED, 0, 65535),
                typeInfo(MNumeric.MEDIUMINT, -8388608, 0, 8388607),
                typeInfo(MNumeric.MEDIUMINT_UNSIGNED, 0, 16777216),
                typeInfo(MNumeric.INT, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),
                typeInfo(MNumeric.INT_UNSIGNED, 0L, 1L << 32),
                typeInfo(MNumeric.BIGINT, Long.MIN_VALUE, 0, Long.MAX_VALUE),
                typeInfo(MNumeric.BIGINT_UNSIGNED, 0L, UnsignedLongs.MAX_VALUE),

                // Decimal default is (10,0)
                typeInfo(MNumeric.DECIMAL,
                         new BigDecimal("-9999999999"),
                         new BigDecimal("0000000000"),
                         new BigDecimal("9999999999")),
                typeInfo(MNumeric.DECIMAL_UNSIGNED,
                         new BigDecimal("0000000000"),
                         new BigDecimal("9999999999")),

                typeInfo(MDateAndTime.DATE,
                         MDateAndTime.encodeDate(0, 0, 0),
                         MDateAndTime.encodeDate(9999, 12, 31)),
                typeInfo(MDateAndTime.DATETIME,
                         MDateAndTime.encodeDateTime(1000, 1, 1, 0, 0, 0),
                         MDateAndTime.encodeDateTime(9999, 12, 31, 23, 59, 59)),
                typeInfo(MDateAndTime.TIME,
                         MDateAndTime.encodeTime(-838, 59, 59, null),
                         MDateAndTime.encodeTime(838, 59, 59, null)),
                typeInfo(MDateAndTime.TIMESTAMP, 0, Integer.MAX_VALUE),
                typeInfo(MDateAndTime.YEAR, 0, 255),

                // Just one of the binary types
                typeInfo(MBinary.BINARY, new byte[] { (byte)0x00 }, new byte[] { (byte)0xFF })
            ),
            Arrays.asList(
                MBinary.VARBINARY,
                MBinary.TINYBLOB,
                MBinary.BLOB,
                MBinary.MEDIUMBLOB,
                MBinary.LONGBLOB,
                MString.CHAR,
                MString.VARCHAR,
                MString.TINYTEXT,
                MString.TEXT,
                MString.MEDIUMTEXT,
                MString.LONGTEXT
            )
        );
    }

    public MTypesComparisonTest(String name, Value a, Value b, int expected) throws Exception {
        super(name, a, b, expected);
    }
}
