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

package com.akiban.server.types;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.LinkedConversion;
import com.akiban.server.types.typestests.TestCase;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class ObjectConversionTest extends ConversionTestBase<Object> {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        Collection<TestCase<?>> testCases =  TestCase.collect(
                TestCase.<Object>forLong(1L, 1L),
                TestCase.<Object>forString("foo", "bar")
        );

        ParameterizationBuilder builder = new ParameterizationBuilder();
        for (TestCase<?> testCase : testCases) {
            builder.add(testCase.toString(), testCase);
        }
        return builder.asList();
    }

    public ObjectConversionTest(TestCase<Object> testCase) {
        super(new ObjectConversionLink(), testCase);
    }

    private static class ObjectConversionLink implements LinkedConversion<Object> {
        @Override
        public ConversionSource source() {
            return source;
        }

        @Override
        public ConversionTarget target() {
            return target;
        }

        @Override
        public void checkPut(Object expected) {
            assertEquals("last converted object", expected, target.lastConvertedValue());
        }

        @Override
        public void setUp(AkType type) {
            target.expectType(type);
        }

        @Override
        public void syncConversions() {
            source.setReflectively(target.lastConvertedValue());
        }

        private final FromObjectConversionSource source = new FromObjectConversionSource();
        private final ToObjectConversionTarget target = new ToObjectConversionTarget();
    }
}
