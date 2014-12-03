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

package com.foundationdb.server.types.service;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

import java.util.Arrays;

public class TestTypesRegistry extends TypesRegistry
{
    public TestTypesRegistry(TClass... tclasses) {
        super(Arrays.asList(tclasses));
    }

    // TODO: Distinguish tests that want MCOMPAT from those that just want normal types.
    public static final TypesRegistry MCOMPAT = 
        // These are the types used by unit tests before services.
        new TestTypesRegistry(MNumeric.INT, MNumeric.BIGINT, MNumeric.SMALLINT, MNumeric.TINYINT,
                              MNumeric.DECIMAL, MNumeric.DECIMAL_UNSIGNED,
                              MApproximateNumber.DOUBLE, MApproximateNumber.FLOAT,
                              MDateAndTime.DATE, MDateAndTime.DATETIME, MDateAndTime.TIMESTAMP,
                              MDateAndTime.YEAR,
                              MString.CHAR, MString.VARCHAR, MString.TEXT,
                              MBinary.VARBINARY);
}
