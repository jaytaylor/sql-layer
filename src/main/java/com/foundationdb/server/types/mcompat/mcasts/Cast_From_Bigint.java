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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TCastBase;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

import static com.foundationdb.server.types.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Bigint
{
    
    /**
     * TODO:
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
    
    public static final TCast TO_TINYINT = new FromInt64ToInt8(MNumeric.BIGINT, MNumeric.TINYINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_TINYINT = new FromInt64ToUnsignedInt8(MNumeric.BIGINT, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_SMALLINT = new FromInt64ToInt16(MNumeric.BIGINT, MNumeric.SMALLINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_SMALLINT = new FromInt64ToUnsignedInt16(MNumeric.BIGINT, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUM_INT = new FromInt64ToInt32(MNumeric.BIGINT, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt64ToUnsignedInt32(MNumeric.BIGINT, MNumeric.MEDIUMINT_UNSIGNED, false, Constantness.UNKNOWN);

    public static final TCast TO_INT = new FromInt64ToInt32(MNumeric.BIGINT, MNumeric.INT, false, Constantness.UNKNOWN);

    public static final TCast TO_INT_UNSIGNED = new FromInt64ToUnsignedInt32(MNumeric.BIGINT, MNumeric.INT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt64ToInt64(MNumeric.BIGINT, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt64ToDecimal(MNumeric.BIGINT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt64ToDouble(MNumeric.BIGINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DATE = new TCastBase(MNumeric.BIGINT, MDatetimes.DATE, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long ymd[] = MDatetimes.fromDate(source.getInt64());
            if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid datetime values"));
                target.putNull();
            }
            else
                target.putInt32(MDatetimes.encodeDate(ymd));
        }
    };


    public static final TCast TO_DATETIME = new TCastBase(MNumeric.BIGINT, MDatetimes.DATETIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long val = source.getInt64();
            boolean notime = false;
            if (val < 100000000) {
                // this is a YYYY-MM-DD int -- need to pad it with 0's for HH-MM-SS
                val *= 1000000;
                notime = true;
            }
            long ymd[] = MDatetimes.decodeDatetime(val);
            if (notime && (ymd[0] < 100)) {
                // no century given.
                if (ymd[0] < 70)
                    ymd[0] += 2000;
                else
                    ymd[0] += 1900;
                val = MDatetimes.encodeDatetime(ymd);
            }
            if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid datetime values"));
                target.putNull();
            }
            else
                target.putInt64(val);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MNumeric.BIGINT, MDatetimes.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // TIMESTAMPE is underlied by INT32
            target.putInt32((int)MDatetimes.encodeTimetamp(source.getInt64(), context));
        }
    };

    public static final TCast TO_TIME = new TCastBase(MNumeric.BIGINT, MDatetimes.TIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            long raw = source.getInt64();
            long ymd[] = MDatetimes.decodeTime(raw);
                        if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid TIME values: " + raw));
                target.putNull();
            }
            else
                target.putInt32((int)CastUtils.getInRange(MDatetimes.TIME_MAX, MDatetimes.TIME_MIN, raw, context));
        }
    };
}
