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
package com.foundationdb.server.types3.mcompat.mcasts;

import com.foundationdb.server.types3.TCast;
import com.foundationdb.server.types3.TCastBase;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.mcompat.mtypes.MBinary;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;

import java.nio.charset.Charset;

public final class Cast_From_Varbinary {

    private Cast_From_Varbinary() {}

    public static final TCast VARBINARY_TO_BLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.BLOB);
    public static final TCast VARBINARY_TO_LONGBLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.LONGBLOB);
    public static final TCast VARBINARY_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.MEDIUMBLOB);
    public static final TCast VARBINARY_TO_TINYBLOB = new BinaryToBinary(MBinary.VARBINARY, MBinary.TINYBLOB);

    public static final TCast BLOB_TO_VARBINARY = new BinaryToBinary(MBinary.BLOB, MBinary.VARBINARY);
    public static final TCast BLOB_TO_LONGBLOB = new BinaryToBinary(MBinary.BLOB, MBinary.LONGBLOB);
    public static final TCast BLOB_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.BLOB, MBinary.MEDIUMBLOB);
    public static final TCast BLOB_TO_TINYBLOB = new BinaryToBinary(MBinary.BLOB, MBinary.TINYBLOB);

    public static final TCast LONGBLOB_TO_VARBINARY = new BinaryToBinary(MBinary.LONGBLOB, MBinary.VARBINARY);
    public static final TCast LONGBLOB_TO_BLOB = new BinaryToBinary(MBinary.LONGBLOB, MBinary.BLOB);
    public static final TCast LONGBLOB_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.LONGBLOB, MBinary.MEDIUMBLOB);
    public static final TCast LONGBLOB_TO_TINYBLOB = new BinaryToBinary(MBinary.LONGBLOB, MBinary.TINYBLOB);

    public static final TCast MEDIUMBLOB_TO_VARBINARY = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.VARBINARY);
    public static final TCast MEDIUMBLOB_TO_BLOB = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.BLOB);
    public static final TCast MEDIUMBLOB_TO_LONGBLOB = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.LONGBLOB);
    public static final TCast MEDIUMBLOB_TO_TINYBLOB = new BinaryToBinary(MBinary.MEDIUMBLOB, MBinary.TINYBLOB);

    public static final TCast TINYBLOB_TO_VARBINARY = new BinaryToBinary(MBinary.TINYBLOB, MBinary.VARBINARY);
    public static final TCast TINYBLOB_TO_BLOB = new BinaryToBinary(MBinary.TINYBLOB, MBinary.BLOB);
    public static final TCast TINYBLOB_TO_LONGBLOB = new BinaryToBinary(MBinary.TINYBLOB, MBinary.LONGBLOB);
    public static final TCast TINYBLOB_TO_MEDIUMBLOB = new BinaryToBinary(MBinary.TINYBLOB, MBinary.MEDIUMBLOB);

    private static class BinaryToBinary extends TCastBase {
        private BinaryToBinary(TClass sourceClass, TClass targetClass) {
            super(sourceClass, targetClass);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            MBinary.putBytes(context, target, source.getBytes());
        }
    }

    private static final String DEFAULT_CHARSET = Charset.defaultCharset().name();
}
