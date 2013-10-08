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

public class Cast_From_Unsigned_Tinyint
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
    
    public static final TCast TO_TINYINT = new FromInt16ToInt8(MNumeric.TINYINT_UNSIGNED, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_SMALLINT = new FromInt16ToInt16(MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
 
    public static final TCast TO_UNSGINED_SMALLINT = new FromInt16ToInt32(MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN);

    public static final TCast TO_MEDIUMINT = new FromInt16ToInt32(MNumeric.TINYINT_UNSIGNED, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
 
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt16ToInt64(MNumeric.TINYINT_UNSIGNED, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt16ToInt32(MNumeric.TINYINT_UNSIGNED, MNumeric.INT, false, Constantness.UNKNOWN);
   
    public static final TCast TO_UNSIGNED_INT = new FromInt16ToInt64(MNumeric.TINYINT_UNSIGNED, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt16ToDecimal(MNumeric.TINYINT_UNSIGNED, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUGLE = new FromInt16ToDouble(MNumeric.TINYINT_UNSIGNED, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);
}
