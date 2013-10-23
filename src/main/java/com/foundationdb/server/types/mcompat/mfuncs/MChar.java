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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public class MChar extends TScalarBase
{

    public static final TScalar INSTANCE = new MChar();
    
    private MChar(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.vararg(MNumeric.BIGINT);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        // length of the varbin string
        int length = 0;
        
        // legnth of each sub string
        int lengths[] = new int[inputs.size()];
        
        int n = 0;
        for(ValueSource num : inputs)
            length += lengths[n++] = byteLength(num.getInt64());

        byte ret[] = new byte[length];

        int pos = 0;
        for (n = 0; n < lengths.length; ++n)
            parse(inputs.get(n).getInt64(), ret, pos += lengths[n]);
        
        output.putBytes(ret);
    }

    @Override
    public String displayName()
    {
        return "CHAR";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                return MBinary.VARBINARY.instance(inputs.size() * 4, anyContaminatingNulls(inputs));
            }
        });
    }
    
    private static final long MASK = 0xff;
    private static final double DIV = Math.log(256);
    
    private static int byteLength(long num)
    {
        return num < 255 
                    ? 1
                    : (int)(Math.log(num) / DIV + 1); 
    }
    /**
     * TODO: byte is too small ==> causes overflow, but this is what underlies VARBINARY
     * @param num
     * 
     */
    static void parse(long num, byte[] ret, int limit)
    {
        int n = limit -1;
        
        while (num > 0)
        {
            ret[n--] = (byte)(num & MASK);
            num >>= 8;
        }
    }
}
