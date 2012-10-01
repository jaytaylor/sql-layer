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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.math.BigInteger;
import java.util.List;

public abstract class  MExportSet extends TOverloadBase
{
    public static final TOverload INSTANCES[] 
            = createOverloads(MNumeric.INT, MString.VARCHAR, MNumeric.BIGINT_UNSIGNED);
    
    private static final BigInteger MASK = new BigInteger("ffffffffffffffff", 16);
    private static final int DEFAULT_LENGTH = 64;
    private static final String DEFAULT_DELIM = ",";
    
    public static TOverload[] createOverloads(final TClass intType, final TClass stringType, final TClass uBigintType)
    {
        return new TOverload[]
        {
            new MExportSet(stringType) // 3 args case
            {
                @Override
                protected String getDelimeter(LazyList<? extends PValueSource> inputs)
                {
                    return DEFAULT_DELIM;
                }

                @Override
                protected int getLength(LazyList<? extends PValueSource> inputs)
                {
                    return DEFAULT_LENGTH;
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(uBigintType, 0).covers(stringType, 1, 2);
                }
                
            },
            new MExportSet(stringType) // 4 args case
            {

                @Override
                protected String getDelimeter(LazyList<? extends PValueSource> inputs)
                {
                    return inputs.get(3).getString();
                }

                @Override
                protected int getLength(LazyList<? extends PValueSource> inputs)
                {
                    return DEFAULT_LENGTH;
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(uBigintType, 0).covers(stringType, 1, 2, 3);
                }
                
            },
            new MExportSet(stringType) // 5 arg case
            {

                @Override
                protected String getDelimeter(LazyList<? extends PValueSource> inputs)
                {
                    return inputs.get(3).getString();
                }

                @Override
                protected int getLength(LazyList<? extends PValueSource> inputs)
                {
                    return Math.min(DEFAULT_LENGTH, inputs.get(4).getInt32());
                }

                @Override
                protected void buildInputSets(TInputSetBuilder builder)
                {
                    builder.covers(uBigintType, 0).covers(stringType, 1, 2, 3).covers(intType, 4);
                }
            }
        };
    }
            
    private static String computeSet(long num, String bits[], String delim, int length)
    {
        char digits[] = Long.toBinaryString(num).toCharArray();
        int count = 0;
        StringBuilder ret = new StringBuilder();
        
        // return value is in little-endian format
        for (int n = digits.length - 1; n >= 0 && count < length; --n, ++count)
            ret.append(bits[digits[n] - '0']).append(delim);
        
        // fill the rest with 'off'
        for (; count < length; ++count)
            ret.append(bits[0]).append(delim);
        if (!delim.isEmpty()) // delete the last delimiter
            return ret.substring(0, ret.length() - delim.length());
        return ret.toString();
    }

    protected abstract String getDelimeter(LazyList<? extends PValueSource> inputs);
    protected abstract int getLength(LazyList<? extends PValueSource> inputs);

    private final TClass stringType;
    private MExportSet(TClass stringType)
    {
        this.stringType = stringType;
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String s = computeSet(
                inputs.get(0).getInt64(),
                new String[]{inputs.get(2).getString(), inputs.get(1).getString()},
                getDelimeter(inputs),
                getLength(inputs));
        output.putString(s, null);
        
    }
    
    @Override
    public String displayName()
    {
        return "EXPORT_SET";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue on = inputs.get(1);
                TPreptimeValue off;
                
                if (on == null 
                        || (off = inputs.get(2)) == null
                        || on.value().isNull()
                        || off.value().isNull()
                   )
                    return stringType.instance(255); // if not lieteral, the length would just be 255
                
                // compute the length
                
                // get the digits length
                int digitLength = Math.max((on.value().getString()).length(), 
                                            (off.value().getString()).length());
                int length = DEFAULT_LENGTH; // number of digits
                int delimLength = DEFAULT_DELIM.length();
                
                switch(inputs.size())
                {
                    case 5:     
                        if (inputs.get(4) != null && !inputs.get(5).value().isNull())
                            length = inputs.get(5).value().getInt32();  // fall thru
                    case 4:
                        if (inputs.get(3) != null && !inputs.get(3).value().isNull())
                            delimLength = (inputs.get(3).value().getString()).length();
                }
                // There would only be [length - 1] number of delimiter characters
                // in the string. But we'd give it enough space for [length]
                return stringType.instance(length * (digitLength + delimLength));
            }
            
        });
                
    }
    
}
