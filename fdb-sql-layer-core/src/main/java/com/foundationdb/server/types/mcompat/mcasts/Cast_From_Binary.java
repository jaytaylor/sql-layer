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

import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.nio.charset.Charset;

public final class Cast_From_Binary {

    private Cast_From_Binary() {}

    public static final TCast BINARY_TO_VARBINARY = new BinaryToBinary(MBinary.BINARY, MBinary.VARBINARY);
    public static final TCast VARBINARY_TO_BINARY = new BinaryToBinary(MBinary.VARBINARY, MBinary.BINARY);

    private static class BinaryToBinary extends TCastBase {
        private BinaryToBinary(TBinary sourceClass, TBinary targetClass) {
            super(sourceClass, targetClass);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            TBinary.putBytes(context, target, source.getBytes());
        }
    }
}
