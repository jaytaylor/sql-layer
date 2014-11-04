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

package com.foundationdb.server.types.mcompat.mtypes;

import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.sql.types.TypeId;

public final class MBinary extends TBinary {

    public static final MBinary VARBINARY = new MBinary(TypeId.VARBIT_ID, "varbinary", -1);
    public static final MBinary BINARY = new MBinary(TypeId.BIT_ID, "binary", -1);
    public static final MBinary TINYBLOB = new MBinary(TypeId.TINYBLOB_ID, "tinyblob", 255);
    public static final MBinary BLOB = new MBinary(TypeId.BLOB_ID, "blob", 65535);
    public static final MBinary MEDIUMBLOB = new MBinary(TypeId.MEDIUMBLOB_ID, "mediumblob", 16777215);
    public static final MBinary LONGBLOB = new MBinary(TypeId.LONGBLOB_ID, "longblob", Integer.MAX_VALUE); // TODO not big enough!

    private MBinary(TypeId typeId, String name, int defaultLength) {
        super(typeId, MBundle.INSTANCE, name, defaultLength);
    }

}
