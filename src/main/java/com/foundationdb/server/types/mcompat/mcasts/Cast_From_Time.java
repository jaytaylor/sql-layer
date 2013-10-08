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

import com.foundationdb.server.types.*;
import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

public class Cast_From_Time {

   /**
     * TODO:
     * 
     * YEAR
     * TIMESTAMP
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
    
    public static final TCast TO_TINYINT = new FromInt32ToInt8(MDatetimes.TIME, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt32ToUnsignedInt8(MDatetimes.TIME, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_SMALLINT = new FromInt32ToInt16(MDatetimes.TIME, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt32ToUnsignedInt16(MDatetimes.TIME, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt32ToInt32(MDatetimes.TIME, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt32ToUnsignedInt32(MDatetimes.TIME, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt32ToInt32(MDatetimes.TIME, MNumeric.INT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt32ToUnsignedInt32(MDatetimes.TIME, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt32ToInt64(MDatetimes.TIME, MNumeric.BIGINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_BIGINT = new FromInt32ToInt64(MDatetimes.TIME, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_DOUBLE = new FromInt32ToDouble(MDatetimes.TIME, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);

    public static final TCast TO_DECIMAL = new FromInt32ToDecimal(MDatetimes.TIME, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    // Generally, TIME cannot be converted to DATE, DATETIME or TIMESTAMP
    // Any cast from <TIME> --> <DATE> | <DATETIME> | <TIMESTAMP> should result in zeros
    // and a warning.
    // But contrast the similarly named _functions_, which do not give a warning and
    // do preserve the time fields when appropriate.
    public static final TCast TO_DATETIME = new TCastBase(MDatetimes.TIME, MDatetimes.DATETIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // direct cast TIME --> DATETIME results in a truncation to zero
            context.reportTruncate(String.valueOf(source.getInt32()), "0000-00-00 00:00:00");
            target.putInt64(0);
        }
    };
    
    public static final TCast TO_DATE = new TCastBase(MDatetimes.TIME, MDatetimes.DATE, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // direct cast TIME --> DATE results in a truncation to zero
            context.reportTruncate(String.valueOf(source.getInt32()), "0000-00-00");
            target.putInt32(0);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDatetimes.TIME, MDatetimes.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // direct cast TIME --> TIMESTAMP results in a truncation to zero
            context.reportTruncate(String.valueOf(source.getInt32()), "0000-00-00 00:00:00");
            target.putInt32(0);
        }
    };
}
