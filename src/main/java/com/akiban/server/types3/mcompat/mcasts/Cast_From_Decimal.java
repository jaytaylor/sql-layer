/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.math.BigDecimal;

public final class Cast_From_Decimal {

    public static final TCastPath APPROX = TCastPath.create(
            MNumeric.DECIMAL,
            MNumeric.DECIMAL_UNSIGNED,
            MApproximateNumber.FLOAT,
            MApproximateNumber.FLOAT_UNSIGNED,
            MApproximateNumber.DOUBLE,
            MApproximateNumber.DOUBLE_UNSIGNED
    );

    public static final TCast TO_DECIMAL_UNSIGNED = new TCastBase(MNumeric.DECIMAL, MNumeric.DECIMAL_UNSIGNED) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            MBigDecimal.adjustAttrsAsNeeded(context, source, context.outputTInstance(), target);
        }
    };

    public static final TCast TO_FLOAT = new TCastBase(MNumeric.DECIMAL, MApproximateNumber.FLOAT) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            BigDecimalWrapper decimal = MBigDecimal.getWrapper(source, context.inputTInstanceAt(0));
            float asFloat = decimal.asBigDecimal().floatValue();
            target.putFloat(asFloat);
        }
    };

    public static final TCast UNSIGNED_TO_FLOAT = new TCastBase(MNumeric.DECIMAL_UNSIGNED, MApproximateNumber.FLOAT) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            BigDecimalWrapper decimal = MBigDecimal.getWrapper(source, context.inputTInstanceAt(0));
            float asFloat = decimal.asBigDecimal().floatValue();
            target.putFloat(asFloat);
        }
    };

    public static final TCast TO_DOUBLE = new TCastBase(MNumeric.DECIMAL, MApproximateNumber.DOUBLE) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            BigDecimalWrapper decimal = MBigDecimal.getWrapper(source, context.inputTInstanceAt(0));
            double asDouble = decimal.asBigDecimal().doubleValue();
            target.putDouble(asDouble);
        }
    };

    public static final TCast TO_BIGINT = new TCastBase(MNumeric.DECIMAL, MNumeric.BIGINT) {
        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            BigDecimalWrapper wrapped = MBigDecimal.getWrapper(source, context.inputTInstanceAt(0));
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
