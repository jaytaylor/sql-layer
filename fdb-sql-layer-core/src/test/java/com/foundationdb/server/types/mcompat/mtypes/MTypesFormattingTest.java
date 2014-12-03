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

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TypeFormattingTestBase;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber.*;
import static com.foundationdb.server.types.mcompat.mtypes.MBinary.*;
import static com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.*;
import static com.foundationdb.server.types.mcompat.mtypes.MNumeric.*;
import static com.foundationdb.server.types.mcompat.mtypes.MString.*;

@RunWith(Parameterized.class)
public class MTypesFormattingTest extends TypeFormattingTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        List<Object[]> params = new ArrayList<>();
        for(TClass tClass : Arrays.asList(DECIMAL, DECIMAL_UNSIGNED)) {
            params.add(tCase(tClass, new BigDecimal("3.14"), "3.14", "\"3.14\"", "3.14"));
        }
        for(TClass tClass : Arrays.asList(TINYINT, TINYINT_UNSIGNED, SMALLINT, SMALLINT_UNSIGNED, MEDIUMINT, MEDIUMINT_UNSIGNED, INT, INT_UNSIGNED, BIGINT, BIGINT_UNSIGNED)) {
            params.add(tCase(tClass, 42, "42", "42", "42"));
        }
        for(TClass tClass : Arrays.asList(FLOAT, FLOAT_UNSIGNED, DOUBLE, DOUBLE_UNSIGNED)) {
            params.add(tCase(tClass, 3.14, "3.14", "3.14", "3.140000e+00"));
        }
        for(TClass tClass : Arrays.asList(CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT)) {
            params.add(tCase(tClass, "hello", "hello", "\"hello\"", "'hello'"));
        }
        for(TClass tClass : Arrays.asList(BINARY, VARBINARY)) {
            params.add(tCase(tClass, new byte[]{ 0x41, 0x42 }, "AB", "\"\\x4142\"", "X'4142'"));
        }
        params.add(tCase(DATE, 1031372, "2014-06-12", "\"2014-06-12\"", "DATE '2014-06-12'"));
        params.add(tCase(DATETIME, 20140612174400L, "2014-06-12 17:44:00", "\"2014-06-12 17:44:00\"", "TIMESTAMP '2014-06-12 17:44:00'"));
        params.add(tCase(TIME, 10203, "01:02:03", "\"01:02:03\"", "TIME '01:02:03'"));
        params.add(tCase(TIMESTAMP, 1402595040, "2014-06-12 17:44:00", "\"2014-06-12 17:44:00\"", "TIMESTAMP '2014-06-12 17:44:00'"));
        params.add(tCase(YEAR, 100, "2000", "\"2000\"", "2000"));
        return checkParams(MBundle.INSTANCE.id(), params);
    }

    public MTypesFormattingTest(TClass tClass, ValueSource valueSource, String str, String json, String literal) {
        super(tClass, valueSource, str, json, literal);
    }
}
