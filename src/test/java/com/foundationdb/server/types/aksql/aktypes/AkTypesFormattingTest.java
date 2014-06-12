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

package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TypeFormattingTestBase;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AkTypesFormattingTest extends TypeFormattingTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        List<Object[]> params = new ArrayList<>();

        params.add(tCase(AkBool.INSTANCE, true, "true", "true", "TRUE"));
        String guid = "10f11fbe-d9f2-11e3-b96e-7badf4bedd17";
        params.add(tCase(AkGUID.INSTANCE, guid, guid, '"' + guid + '"', "'" + guid + "'"));
        return checkParams(AkBundle.INSTANCE.id(), params, AkResultSet.INSTANCE, AkInterval.MONTHS, AkInterval.SECONDS);
    }

    public AkTypesFormattingTest(TClass tClass, ValueSource valueSource, String str, String json, String literal) {
        super(tClass, valueSource, str, json, literal);
    }
}

