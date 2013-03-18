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
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

@SuppressWarnings("unused") // Used by reflected
public class Cast_From_Boolean {

    public static final TCast BOOL_TO_INTEGER = new TCastBase(AkBool.INSTANCE, MNumeric.INT) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putInt32(source.getBoolean() ? 1 : 0);
        }
    };

    // MApproximateNumber
    public static final TCast FLOAT_TO_BOOLEAN = new TCastBase(MApproximateNumber.FLOAT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getFloat() != 0.0);
        }
    };
    public static final TCast FLOAT_UNSIGNED_TO_BOOLEAN = new TCastBase(MApproximateNumber.FLOAT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getFloat() != 0.0);
        }
    };
    public static final TCast DOUBLE_TO_BOOLEAN = new TCastBase(MApproximateNumber.DOUBLE, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getDouble() != 0.0);
        }
    };
    public static final TCast DOUBLE_UNSIGNED_TO_BOOLEAN = new TCastBase(MApproximateNumber.DOUBLE_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getDouble() != 0.0);
        }
    };


    // MBinary
    // TODO

    // MDatetimesfinal
    public static final TCast DATE_TO_BOOLEAN = new TCastBase(MDatetimes.DATE, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast DATETIME_TO_BOOLEAN = new TCastBase(MDatetimes.DATETIME, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast TIME_TO_BOOLEAN = new TCastBase(MDatetimes.TIME, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast YEAR_TO_BOOLEAN = new TCastBase(MDatetimes.YEAR, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt16() != 0);
        }
    };
    public static final TCast TIMESTAMP_TO_BOOLEAN = new TCastBase(MDatetimes.TIMESTAMP, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };

    // MNumeric
    public static final TCast TINYINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.TINYINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt16() != 0);
        }
    };
    public static final TCast SMALLINT_TO_BOOLEAN = new TCastBase(MNumeric.SMALLINT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt16() != 0);
        }
    };
    public static final TCast SMALLINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.SMALLINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast MEDIUMINT_TO_BOOLEAN = new TCastBase(MNumeric.MEDIUMINT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast MEDIUMINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.MEDIUMINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast INT_TO_BOOLEAN = new TCastBase(MNumeric.INT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt32() != 0);
        }
    };
    public static final TCast INT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.INT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast BIGINT_TO_BOOLEAN = new TCastBase(MNumeric.BIGINT, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast BIGINT_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.BIGINT_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putBool(source.getInt64() != 0);
        }
    };
    public static final TCast DECIMAL_TO_BOOLEAN = new TCastBase(MNumeric.DECIMAL, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            BigDecimalWrapper decimal = MBigDecimal.getWrapper(source, context.inputTInstanceAt(0));
            target.putBool(!decimal.isZero());
        }
    };
    public static final TCast DECIMAL_UNSIGNED_TO_BOOLEAN = new TCastBase(MNumeric.DECIMAL_UNSIGNED, AkBool.INSTANCE) {
        @Override
        protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            BigDecimalWrapper decimal = MBigDecimal.getWrapper(source, context.inputTInstanceAt(0));
            target.putBool(!decimal.isZero());
        }
    };

    // MString
    // Handled by string->boolean (i.e. TParsers.BOOLEAN)
}