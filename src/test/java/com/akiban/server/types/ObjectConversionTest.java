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
import com.akiban.server.types.typestests.ConversionSuite;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.LinkedConversion;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public final class ObjectConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ObjectConversionLink()).suite();
        return params(suite);
    }

    public ObjectConversionTest(ConversionSuite<?> suite, int i) {
        super(suite, i);
    }

    private static class ObjectConversionLink implements LinkedConversion<Object> {
        @Override
        public ConversionSource linkedSource() {
            return source;
        }

        @Override
        public ConversionTarget linkedTarget() {
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
            source.setExplicitly(target.lastConvertedValue(), target.getConversionType());
        }

        private final FromObjectConversionSource source = new FromObjectConversionSource();
        private final ToObjectConversionTarget target = new ToObjectConversionTarget();
    }
}
