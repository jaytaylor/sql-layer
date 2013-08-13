/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.types.util;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.typestests.ConversionSuite;
import com.foundationdb.server.types.typestests.ConversionTestBase;
import com.foundationdb.server.types.typestests.SimpleLinkedConversion;
import com.foundationdb.server.types.typestests.TestCase;

import java.util.Collection;

/**
 * Parameterised tests for ValueHolder
 */
public final class ValueHolderTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ValueHolderLink()).suite();
        return params(suite);
    }

    public ValueHolderTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }
    
    private static class ValueHolderLink extends SimpleLinkedConversion {
        @Override
        public ValueSource linkedSource() {
            return valueHolder;
        }

        @Override
        public ValueTarget linkedTarget() {
            return valueHolder;
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            valueHolder.requireForPuts(testCase.type());
        }

        private final ValueHolder valueHolder = new ValueHolder();
    }
}
