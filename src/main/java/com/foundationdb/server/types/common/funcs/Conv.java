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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.math.BigInteger;
import java.util.List;

public class Conv extends TScalarBase
{
    static final int MIN_BASE = 2 ;
    static final int MAX_BASE = 36;
    
    // When second argument (toBase) and third argument (fromBase) are literal,
    // it's possible to compute the exact precision of the output string.
    //
    // But when they are not, this is the largest possible length expansion
    // of a string converted from base x to base y where x,y ∈ [MIN_BASE, MAX_BASE]
    private static final double MAX_RATIO = Math.log(MAX_BASE) / Math.log(MIN_BASE);
    private static final BigInteger N64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);
    
    private final TClass stringType;
    private final TClass bigIntType;

    public Conv(TClass stringType, TClass bigIntType)
    {
        assert bigIntType.underlyingType() == UnderlyingType.INT_32 : "expecting INT_32";
        assert stringType.underlyingType() == UnderlyingType.STRING : "expecting STRING";

        this.stringType = stringType;
        this.bigIntType = bigIntType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(stringType, 0).covers(bigIntType, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String st = inputs.get(0).getString();
        int fromBase = inputs.get(1).getInt32();
        int toBase = inputs.get(2).getInt32();

        if (st.isEmpty()
                || !isInRange(fromBase, MIN_BASE, MAX_BASE) 
                || !isInRange(Math.abs(toBase), MIN_BASE, MAX_BASE)) // toBase can be negative
            output.putNull();
        else
            output.putString(doConvert(st, fromBase, toBase), null);
    }

    @Override
    public String displayName()
    {
        return "CONV";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue fromBase = inputs.get(1);
                TPreptimeValue toBase = inputs.get(2);
                
                int strPre = context.inputTypeAt(0).attribute(StringAttribute.MAX_LENGTH);

                // if toBase and fromBase are not available yet,
                // use the default value of ratio
                if (isNull(fromBase) || isNull(toBase))
                    return stringType.instance((int)(strPre * MAX_RATIO + 1), anyContaminatingNulls(inputs));

                // compute the exact length of the converted string
                strPre = (int)(strPre * Math.log(toBase.value().getInt32()) 
                                        / Math.log(fromBase.value().getInt32()));
                return stringType.instance(strPre, anyContaminatingNulls(inputs));
            }
            
        });
    }

    private static boolean isNull(TPreptimeValue val)
    {
        return val == null || val.value() == null;
    }
    
    private static boolean isInRange (int num, int min, int max)
    {
        return num <= max && num >= min;
    }
    
    public static String truncateNonDigits(String st)
    {
        StringBuilder ret = new StringBuilder();
        int index = 0;
        char ch;
        for(; index < st.length(); ++index)
            if (!Character.isDigit(ch = st.charAt(index)))
                return ret.toString();
            else 
                ret.append(ch);
        return ret.toString();
    }
    
    /**
     * 
     * @param st: numeric string
     * @return a string representing the value in st in toBase.
     *         "0" if the input string is invalid in the given base
     * 
     * if toBase is unsigned, the value contained in st would
     * be interpreted as an unsigned value
     * (Thus, -1 would be the same as FFFFFFFFFFFFFFFF)
     */
    private static String doConvert(String st, int fromBase, int toBase)
    {
        // truncate whatever is after the decimal piont
        for (int n = 0; n < st.length(); ++n)
            if (st.charAt(n) == '.')
            {
                if (n == 0)
                    return "0";
                st = st.substring(0, n);
                break;
            }

        boolean signed = toBase < 0;
        if (signed)
            toBase = -toBase;

        try
        {
            BigInteger num = new BigInteger(st, fromBase);
            // if the number is signed and the toBase value is unsigned
            // interpret the number as unsigned
            if (!signed && num.signum() < 0)
                num = num.abs().xor(N64).add(BigInteger.ONE);

            // cap the output to <= N64
            if (num.compareTo(N64) > 0)
                num = N64;

            return num.toString(toBase).toUpperCase();
        }
        catch (NumberFormatException e)
        {
            return "0";
        }
     }
}
