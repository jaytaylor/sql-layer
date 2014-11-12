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
    public static final TCast BINARY_TO_BLOB = new BinaryToBinary(MBinary.BINARY, MBinary.BLOB);
    public static final TCast BINARY_TO_LONGBLOB = new BinaryToBinary(MBinary.BINARY, MBinary.LONGBLOB);
    public static final TCast BINARY_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.BINARY, MBinary.MEDIUMBLOB);
    public static final TCast BINARY_TO_TINYBLOB = new BinaryToBinary(MBinary.BINARY, MBinary.TINYBLOB);

    public static final TCast VARBINARY_TO_BINARY = new BinaryToBinary(MBinary.VARBINARY, MBinary.BINARY);
    public static final TCast VARBINARY_TO_BLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.BLOB);
    public static final TCast VARBINARY_TO_LONGBLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.LONGBLOB);
    public static final TCast VARBINARY_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.MEDIUMBLOB);
    public static final TCast VARBINARY_TO_TINYBLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.TINYBLOB);

    public static final TCast BLOB_TO_BINARY = new BinaryToBinary(MBinary.BLOB, MBinary.BINARY);
    public static final TCast BLOB_TO_VARBINARY = new BinaryToBinary(MBinary.BLOB, MBinary.VARBINARY);
    public static final TCast BLOB_TO_LONGBLOB = new BinaryToBinary(MBinary.BLOB, MBinary.LONGBLOB);
    public static final TCast BLOB_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.BLOB, MBinary.MEDIUMBLOB);
    public static final TCast BLOB_TO_TINYBLOB = new BinaryToBinary(MBinary.BLOB, MBinary.TINYBLOB);

    public static final TCast LONGBLOB_TO_BINARY = new BinaryToBinary(MBinary.LONGBLOB, MBinary.BINARY);
    public static final TCast LONGBLOB_TO_VARBINARY = new BinaryToBinary(MBinary.LONGBLOB, MBinary.VARBINARY);
    public static final TCast LONGBLOB_TO_BLOB = new BinaryToBinary(MBinary.LONGBLOB, MBinary.BLOB);
    public static final TCast LONGBLOB_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.LONGBLOB, MBinary.MEDIUMBLOB);
    public static final TCast LONGBLOB_TO_TINYBLOB = new BinaryToBinary(MBinary.LONGBLOB, MBinary.TINYBLOB);

    public static final TCast MEDIUMBLOB_TO_BINARY = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.BINARY);
    public static final TCast MEDIUMBLOB_TO_VARBINARY = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.VARBINARY);
    public static final TCast MEDIUMBLOB_TO_BLOB = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.BLOB);
    public static final TCast MEDIUMBLOB_TO_LONGBLOB = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.LONGBLOB);
    public static final TCast MEDIUMBLOB_TO_TINYBLOB = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.TINYBLOB);

    public static final TCast TINYBLOB_TO_BINARY = new BinaryToBinary(MBinary.TINYBLOB, MBinary.BINARY);
    public static final TCast TINYBLOB_TO_VARBINARY = new BinaryToBinary(MBinary.TINYBLOB, MBinary.VARBINARY);
    public static final TCast TINYBLOB_TO_BLOB = new BinaryToBinary(MBinary.TINYBLOB, MBinary.BLOB);
    public static final TCast TINYBLOB_TO_LONGBLOB = new BinaryToBinary(MBinary.TINYBLOB, MBinary.LONGBLOB);
    public static final TCast TINYBLOB_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.TINYBLOB, MBinary.MEDIUMBLOB);

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
