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

package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.types.ErrorHandlingMode;
import com.foundationdb.server.types.TExecutionContext;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class CastUtilsTest {
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        param(pb, "12.5", 13);
        param(pb, "-1299.5", -1300);
        param(pb, "2.0", 2);
        param(pb, "-2.0", -2);
        param(pb, "2.b", 2);
        param(pb, "2.4", 2);
        param(pb, "2.5", 3);
        param(pb, "-2.5", -3);
        param(pb, "", 0);
        param(pb, "a", 0);
        param(pb, "-", 0);
        param(pb, ".", 0);
        param(pb, "-.3", 0);
        param(pb, "-.6", -1);
        param(pb, "+.3", 0);
        param(pb, "+.6", 1);
        param(pb, ".6", 1);
        param(pb, ".6E4", 6000);
        param(pb, "123E4", 1230000);
        param(pb, "123E-4", 0);
        param(pb, "467E-3", 0);
        param(pb, "567E-3", 1);
        param(pb, "123.456E3", 123456);
        param(pb, "27474.83647e-4", 3);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, String input, long expected) {
        pb.add(input.length() == 0 ? "<empty>" : input, input, expected);
    }

    @Test
    public void testTruncate() {
        TExecutionContext context = new TExecutionContext(null, null, null, null,
                ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE, ErrorHandlingMode.IGNORE);
        long actual = CastUtils.parseInRange(input, Long.MAX_VALUE, Long.MIN_VALUE, context);
    
        assertEquals(input, expected, actual);
    }

    public CastUtilsTest(String input, long expected) {
        this.input = input;
        this.expected = expected;
    }

    private final String input;
    private final long expected;
}
