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
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO BITNOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * BITNOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class MBinaryBit extends TOverloadBase {

    static enum BitOperator 
    {
        BITAND
        {
            @Override
            public String displayName()
            {
                return "&";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                return a0 & a1;
            }
        },
        BITOR
        {
            @Override
            public String displayName()
            {
                return "|";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                return a0 | a1;
            }
        },
        BITXOR
        {
            @Override
            public String displayName()
            {
                return "^";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                return a0 ^ a1;
            }
        },
        LEFTSHIFT
        {
            @Override
            public String displayName()
            {
                return "<<";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                return a0 << a1;
            }
        },
        RIGHTSHIFT
        {
            @Override
            public String displayName()
            {
                return ">>";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                return a0 >> a1;
            }
        },
        BITNOT
        {
            @Override
            public String displayName()
            {
                return "!";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                // doEvaluate method overrides this
                return -1;
            }
        },
        BIT_COUNT
        {
            @Override
            public String displayName()
            {
                return "bit_count";
            }

            @Override
            long evaluate(long a0, long a1)
            {
                // doEvaluate method overrides this
                return -1;
            }
        };

        abstract long evaluate(long a0, long a1);

        abstract String displayName();
    }
    
    public static final TOverload[] INSTANCES = {
        new MBinaryBit(BitOperator.BITAND),
        new MBinaryBit(BitOperator.BITOR),
        new MBinaryBit(BitOperator.BITXOR),
        new MBinaryBit(BitOperator.LEFTSHIFT),
        new MBinaryBit(BitOperator.RIGHTSHIFT),
        new MBinaryBit(BitOperator.BITNOT) {           
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MNumeric.BIGINT_UNSIGNED, 0);
            }
            
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                output.putInt64(~inputs.get(0).getInt64());
            }
        },
        new MBinaryBit(BitOperator.BIT_COUNT) {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MNumeric.BIGINT_UNSIGNED, 0);
            }
            
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                long input = inputs.get(0).getInt64();
                output.putInt64(Long.bitCount(input));
            }
        }
    };
    
    private final BitOperator bitOp;
    private final String registeredNames [];
    private MBinaryBit(BitOperator bitOp) {
        this.bitOp = bitOp;
        registeredNames  = new String[]{bitOp.name()};
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MNumeric.BIGINT_UNSIGNED, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        long a0 = inputs.get(0).getInt64();
        long a1 = inputs.get(1).getInt64();
        long ret = bitOp.evaluate(a0, a1);
        output.putInt64(ret);
    }

    @Override
    public String displayName() {
        return bitOp.displayName();
    }

    @Override
    public String[] registeredNames()
    {
        return registeredNames;
    }
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT_UNSIGNED.instance());
    }
}
