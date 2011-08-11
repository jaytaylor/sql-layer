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

package com.akiban.server.rowdata;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
import com.akiban.server.types.typestests.LinkedConversion;

public final class RowDataConversionTest {

    private static final class ConversionPair implements LinkedConversion<Long> {
        @Override
        public ConversionSource linkedSource() {
            return source;
        }

        @Override
        public ConversionTarget linkedTarget() {
            return target;
        }

        @Override
        public void checkPut(Long expected) {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public void setUp(AkType type) {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public void syncConversions() {
            throw new UnsupportedOperationException(); // TODO
        }

        private final RowDataConversionSource source = new RowDataConversionSource();
        private final RowDataConversionTarget target = new RowDataConversionTarget();
    }
}
