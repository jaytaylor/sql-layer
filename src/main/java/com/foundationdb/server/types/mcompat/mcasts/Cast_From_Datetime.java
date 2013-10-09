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

import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Datetime
{
    public static final TCast TO_TINYINT = new FromInt64ToInt8(MDatetimes.DATETIME, MNumeric.TINYINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_TINYINT = new FromInt64ToUnsignedInt8(MDatetimes.DATETIME, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_SMALLINT = new FromInt64ToInt16(MDatetimes.DATETIME, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALINT = new FromInt64ToUnsignedInt16(MDatetimes.DATETIME, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt64ToInt32(MDatetimes.DATETIME, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt64ToUnsignedInt32(MDatetimes.DATETIME, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt64ToInt32(MDatetimes.DATETIME, MNumeric.INT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt64ToUnsignedInt32(MDatetimes.DATETIME, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt64ToInt64(MDatetimes.DATETIME, MNumeric.BIGINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt64ToInt64(MDatetimes.DATETIME, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt64ToDouble(MDatetimes.DATETIME, MApproximateNumber.DOUBLE, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt64ToDecimal(MDatetimes.DATETIME, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DATE = new TCastBase(MDatetimes.DATETIME, MDatetimes.DATE, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeDate(MDatetimes.decodeDatetime(source.getInt64())));
        }
    };
    
    public static final TCast TO_TIME = new TCastBase(MDatetimes.DATETIME, MDatetimes.TIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeTime(MDatetimes.decodeDatetime(source.getInt64())));
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDatetimes.DATETIME, MDatetimes.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeTimestamp(MDatetimes.decodeDatetime(source.getInt64()),
                                                       context.getCurrentTimezone(),
                                                       context));
        }
    };
}
