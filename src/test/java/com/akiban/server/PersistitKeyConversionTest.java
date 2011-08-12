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

package com.akiban.server;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
import com.akiban.server.types.typestests.ConversionSuite;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.SimpleLinkedConversion;
import com.persistit.Key;
import com.persistit.Persistit;

import java.util.Collection;

public final class PersistitKeyConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new KeyConversionPair()).suite();
        return params(suite);
    }

    public PersistitKeyConversionTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }

    private static final class KeyConversionPair extends SimpleLinkedConversion {
        @Override
        public ConversionSource linkedSource() {
            return source;
        }

        @Override
        public ConversionTarget linkedTarget() {
            return target;
        }

        @Override
        public void setUp(AkType type) {
            key.clear();
            target.attach(key);
            target.expectingType(type);
            source.attach(key, 0, type);
        }

        private final Key key = new Key((Persistit)null);
        private final PersistitKeyConversionTarget target = new PersistitKeyConversionTarget();
        private final PersistitKeyConversionSource source = new PersistitKeyConversionSource();
    }
}
