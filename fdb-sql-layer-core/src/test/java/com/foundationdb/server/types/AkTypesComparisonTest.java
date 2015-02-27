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
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.aksql.aktypes.AkBlob;
import com.foundationdb.server.types.aksql.aktypes.AkGeometry;
import com.foundationdb.server.types.aksql.aktypes.AkInterval;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.value.Value;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

@RunWith(SelectedParameterizedRunner.class)
public class AkTypesComparisonTest extends TypeComparisonTestBase
{
    @Parameters(name="{0}")
    public static Collection<Object[]> types() throws Exception {
        return makeParams(
            AkBundle.INSTANCE.id(),
            Arrays.asList(
                typeInfo(AkBool.INSTANCE, false, true),
                typeInfo(AkInterval.MONTHS, Long.MIN_VALUE, 0L, Long.MAX_VALUE),
                typeInfo(AkInterval.SECONDS, Long.MIN_VALUE, 0L, Long.MAX_VALUE)
            ),
            Arrays.asList(
                AkGeometry.INSTANCE,
                AkGUID.INSTANCE,
                AkResultSet.INSTANCE,
                AkBlob.INSTANCE    
            )
        );
    }

    public AkTypesComparisonTest(String name, Value a, Value b, int expected) throws Exception {
        super(name, a, b, expected);
    }
}
