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
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.math.BigDecimal;

public final class Cast_From_Decimal {

    public static final TCast TO_DECIMAL_UNSIGNED = new TCastBase(MNumeric.DECIMAL, MNumeric.DECIMAL_UNSIGNED) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            TBigDecimal.adjustAttrsAsNeeded(context, source, context.outputType(), target);
        }
    };

    public static final TCast TO_DECIMAL = new TCastBase(MNumeric.DECIMAL_UNSIGNED, MNumeric.DECIMAL) {
        @Override
        protected void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            TBigDecimal.adjustAttrsAsNeeded(context, source, context.outputType(), target);
        }
    };

    public static final TCast TO_FLOAT = new TCastBase(MNumeric.DECIMAL, MApproximateNumber.FLOAT) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper decimal = TBigDecimal.getWrapper(source, context.inputTypeAt(0));
            float asFloat = decimal.asBigDecimal().floatValue();
            target.putFloat(asFloat);
        }
    };

    public static final TCast TO_DOUBLE = new TCastBase(MNumeric.DECIMAL, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper decimal = TBigDecimal.getWrapper(source, context.inputTypeAt(0));
            double asDouble = decimal.asBigDecimal().doubleValue();
            target.putDouble(asDouble);
        }
    };

    public static final TCast TO_BIGINT = new TCastBase(MNumeric.DECIMAL, MNumeric.BIGINT) {
        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
            BigDecimalWrapper wrapped = TBigDecimal.getWrapper(source, context.inputTypeAt(0));
            BigDecimal bd = wrapped.asBigDecimal();
            int signum = bd.signum();
            long longVal;
            if ( (signum < 0) && (bd.compareTo(LONG_MINVAL) < 0)) {
                context.reportTruncate(bd.toString(), Long.toString(LONG_MIN_TRUNCATE));
                longVal = LONG_MIN_TRUNCATE;
            }
            else if ( (signum > 0) && (bd.compareTo(LONG_MAXVAL) > 0)) {
                context.reportTruncate(bd.toString(), Long.toString(LONG_MAX_TRUNCATE));
                longVal = LONG_MAX_TRUNCATE;
            }
            else {
                // TODO make sure no loss of precision causes an error
                longVal = bd.longValue();
            }
            target.putInt64(longVal);
        }
    };

    private Cast_From_Decimal() {}

    private static BigDecimal LONG_MAXVAL = BigDecimal.valueOf(Long.MAX_VALUE);
    private static BigDecimal LONG_MINVAL = BigDecimal.valueOf(Long.MIN_VALUE);
    private static long LONG_MAX_TRUNCATE = Long.MAX_VALUE;
    private static long LONG_MIN_TRUNCATE = Long.MIN_VALUE;
}
