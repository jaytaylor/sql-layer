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
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import java.math.BigInteger;

public class Cast_From_Bigint
{
    
    /**
     * BIT
     * TINYINT
     * TINYINT_U
     * SMALLINT
     * SMALLINT_U
     * MEDIUMINT
     * \MEDIUMINT_U
     * INTEGER
     * INTEGER_U
     * BIGINT
     * BIGINT_U
     * DECIMAL
     * FLOAT
     * DOUBLE
     * DATE
     * DATETIME
     * TIME
     * TIMESTAMP
     * TIMESTAMP
     * YEAR
     * CHAR
     * VARCHAR
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
     * @return 
     */
    public static final TCast TO_TINYINT = new TCastBase(MNumeric.BIGINT, MNumeric.TINYINT, false, Constantness.UNKNOWN)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            long bigint = source.getInt64();
            if (bigint > Byte.MAX_VALUE)
                target.putInt8(Byte.MAX_VALUE);
            else if (bigint < Byte.MIN_VALUE)
                target.putInt8(Byte.MIN_VALUE);
            else
                target.putInt8((byte)bigint);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            return specifiedTarget;
        }
    };
    
    public static final TCast TO_UNSIGNED_TINYINT = new TCastBase(MNumeric.BIGINT, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TODO: take the two's complement of the negative value?
            long bigint = source.getInt64();
            if (bigint > Byte.MAX_VALUE)
                target.putInt8(Byte.MAX_VALUE);
            else if (bigint < Byte.MIN_VALUE)
                target.putInt8(Byte.MIN_VALUE);
            else
                target.putInt8((byte)bigint);
            
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            return specifiedTarget;
        }
    };
        
    //TODO: add more
    private static final BigInteger MAX = new BigInteger("18446744073709551615");
}
