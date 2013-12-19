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

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.sql.types.TypeId;

public final class MBinary extends TBinary {

    public static final TClass VARBINARY = new MBinary(TypeId.VARBIT_ID, "varbinary", -1);
    public static final TClass BINARY = new MBinary(TypeId.BIT_ID, "binary", -1);
    public static final TClass TINYBLOB = new MBinary(TypeId.BLOB_ID, "tinyblob", 256);
    public static final TClass MEDIUMBLOB = new MBinary(TypeId.BLOB_ID, "mediumblob", 65535);
    public static final TClass BLOB = new MBinary(TypeId.BLOB_ID, "blob", 16777215);
    public static final TClass LONGBLOB = new MBinary(TypeId.BLOB_ID, "longblob", Integer.MAX_VALUE); // TODO not big enough!

    private MBinary(TypeId typeId, String name, int defaultLength) {
        super(typeId, name, defaultLength);
    }

}
