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
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Tinyint
{
    /**
     * TODO:
     * 
     * DATE
     * DATETIME
     * TIME
     * TIMESTAMP
     * YEAR
     * 
     * BIT
     * CHAR
     * BINARY
     * VARBINARY
     * TINYBLOG
     * TINYTEXT
     * TEXT
     * MEDIUMBLOB
     * MEDIUMTEXT
     * LONGBLOG
     * LONTTEXT
     * 
     */
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt8ToUnsignedInt8(MNumeric.TINYINT, MNumeric.TINYINT_UNSIGNED, true, Constantness.UNKNOWN);
   
    public static final TCast TO_SMALLINT = new FromInt8ToInt16(MNumeric.TINYINT, MNumeric.SMALLINT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt8ToUnsignedInt16(MNumeric.TINYINT, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_MEDIUMINT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt8ToUnsignedInt32(MNumeric.TINYINT, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_INT = new FromInt8ToInt32(MNumeric.TINYINT, MNumeric.INT, true, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_INT = new FromInt8ToUnsignedInt32(MNumeric.TINYINT, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt8ToInt64(MNumeric.TINYINT, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt8ToDecimal(MNumeric.TINYINT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt8ToDouble(MNumeric.TINYINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
}
