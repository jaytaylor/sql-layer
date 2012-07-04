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

import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

import static com.akiban.server.types3.mcompat.mcasts.MNumericCastBase.*;

public class Cast_From_Date
{
    /**
     * TODO:
     * 
     * TIME
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
   public static final TCast TO_TINYINT = new FromInt32ToInt8(MDatetimes.DATE, MNumeric.TINYINT, false, Constantness.UNKNOWN);

    public static final TCast TO_UNSIGNED_TINYINT = new FromInt32ToInt16(MDatetimes.DATE, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_SMALLINT = new FromInt32ToInt16(MDatetimes.DATE, MNumeric.SMALLINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_SMALINT = new FromInt32ToInt32(MDatetimes.DATE, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN);
    
    public static final TCast TO_MEDIUMINT = new FromInt32ToInt32(MDatetimes.DATE, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_MEDIUMINT = new FromInt32ToInt64(MDatetimes.DATE, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_INT = new FromInt32ToInt32(MDatetimes.DATE, MNumeric.INT, false, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_INT = new FromInt32ToInt64(MDatetimes.DATE, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_BIGINT = new FromInt32ToInt64(MDatetimes.DATE, MNumeric.BIGINT, true, Constantness.UNKNOWN);
    
    public static final TCast TO_UNSIGNED_BIGINT = new FromInt32ToInt64(MDatetimes.DATE, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DOUBLE = new FromInt32ToDouble(MDatetimes.DATE, MApproximateNumber.DOUBLE, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DECIMAL = new FromInt32ToDecimal(MDatetimes.DATE, MNumeric.DECIMAL, true, Constantness.UNKNOWN);
    
    public static final TCast TO_DATETIME = new TCastBase(MDatetimes.DATE, MDatetimes.DATETIME, true, Constantness.UNKNOWN)
    {

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(MDatetimes.decodeDate(source.getInt32())));
        }
    };
    
    public static final TCast TO_TIME = new TCastBase(MDatetimes.DATE, MDatetimes.TIME, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // DATE doesn't have any TIME
            target.putInt32(0);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDatetimes.DATETIME, MDatetimes.TIMESTAMP, false, Constantness.UNKNOWN)
    {

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeTimestamp(MDatetimes.decodeDate(source.getInt32()),
                                                       context.getCurrentTimezone(),
                                                       context));
        }
    };
    
    public static final TCast TO_VARCHAR = new TCastBase(MDatetimes.DATETIME, MString.VARCHAR, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(MDatetimes.dateToString(source.getInt32()));
        }
    };
}
