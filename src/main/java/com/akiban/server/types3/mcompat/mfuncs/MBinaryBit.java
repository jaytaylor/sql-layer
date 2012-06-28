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
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO INVERT_BITS AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * INVERT_BITS USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
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

    static enum BitOperator {
        BITWISE_AND {
            @Override
            long evaluate(long a0, long a1) {
                return a0 & a1;
            }
        }, 
        BITWISE_OR {
            @Override
            long evaluate(long a0, long a1) {
                return a0 | a1;
            }            
        }, 
        BITWISE_XOR {
            @Override
            long evaluate(long a0, long a1) {
                return a0 ^ a1;
            }            
        }, 
        LEFT_SHIFT {
            @Override
            long evaluate(long a0, long a1) {
                return a0 << a1;
            }            
        }, 
        RIGHT_SHIFT {
            @Override
            long evaluate(long a0, long a1) {
                return a0 >> a1;
            }            
        },
        INVERT_BITS {
            @Override
            long evaluate(long a0, long a1) {
                // doEvaluate method overrides this
                return -1;
            }
        };
        abstract long evaluate(long a0, long a1);
    }
    
    public static final TOverload[] INSTANCES = {
        new MBinaryBit(BitOperator.BITWISE_AND),
        new MBinaryBit(BitOperator.BITWISE_OR),
        new MBinaryBit(BitOperator.BITWISE_XOR),
        new MBinaryBit(BitOperator.LEFT_SHIFT),
        new MBinaryBit(BitOperator.RIGHT_SHIFT),
        new MBinaryBit(BitOperator.INVERT_BITS) {           
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MNumeric.BIGINT_UNSIGNED, 0);
            }
            
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                output.putInt64(~inputs.get(0).getInt64());
            }
        }
    };
    
    private final BitOperator bitOp;
   
    private MBinaryBit(BitOperator bitOp) {
        this.bitOp = bitOp;
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
    public String overloadName() {
        return bitOp.name();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT_UNSIGNED.instance());
    }
}
