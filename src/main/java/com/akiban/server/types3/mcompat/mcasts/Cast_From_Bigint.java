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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

import static com.akiban.server.types3.mcompat.mcasts.MNumericCastBase.*;

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

    public static final TCast TO_BIGINT = new FromInt64ToInt64(MNumeric.BIGINT, MNumeric.BIGINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt64ToInt64(MNumeric.BIGINT, MNumeric.BIGINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt64ToDecimal(MNumeric.BIGINT, MNumeric.DECIMAL, false, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt64ToDouble(MNumeric.BIGINT, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DATE = new TCastBase(MNumeric.BIGINT, MDatetimes.DATE, false, Constantness.UNKNOWN)
    {

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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


    public static final TCast TO_DATETIME = new TCastBase(MNumeric.BIGINT, MDatetimes.DATETIME, false, Constantness.UNKNOWN)
    {

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            long raw = source.getInt64();
            long ymd[] = MDatetimes.decodeDatetime(raw);
                        if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid datetime values"));
                target.putNull();
            }
            else
                target.putInt64(raw);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MNumeric.BIGINT, MDatetimes.TIMESTAMP, false, Constantness.UNKNOWN)
    {

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TIMESTAMPE is underlied by INT32
            target.putInt32((int)MDatetimes.encodeTimetamp(source.getInt64(), context));
        }
    };

    public static final TCast TO_TIME = new TCastBase(MNumeric.BIGINT, MDatetimes.TIME, false, Constantness.UNKNOWN)
    {

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
    
    public static final TCast TO_VARCHAR = new TCastBase(MNumeric.BIGINT, MString.VARCHAR, false, Constantness.UNKNOWN)
    {

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Long.toString(source.getInt64()));
        }
    };
    
    public static final TCast TO_AK_BOOLEAN = new TCastBase(MNumeric.BIGINT, AkBool.INSTANCE, false, Constantness.UNKNOWN) {

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            boolean value = source.getInt64() != 0;
            target.putBool(value);
        }
    };
}
