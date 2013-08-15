/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

public class MBinaryBit extends TScalarBase {

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
                return (a1 < 0) ? 0 : (a0 << a1);
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
                return (a1 < 0) ? 0 : (a0 >> a1);
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
                throw new AssertionError();
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
                throw new AssertionError();
            }
        };

        abstract long evaluate(long a0, long a1);

        abstract String displayName();
    }
    
    public static final TScalar[] INSTANCES = {
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
    public String displayName() {
        return bitOp.displayName();
    }

    @Override
    public String[] registeredNames()
    {
        return new String[]{bitOp.name()};
    }
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.BIGINT_UNSIGNED);
    }
}
