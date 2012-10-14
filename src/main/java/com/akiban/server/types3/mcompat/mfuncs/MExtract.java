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
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import java.util.Arrays;

/**
 * 
 * implement TIMESTAMP(<expr>), DATE(<expr>), TIME(<expr>), ... functions
 */
public abstract class MExtract extends TScalarBase
{
    public static TScalar[] create()
    {
        return new TScalar[]
        {
            new MExtract(MDatetimes.DATE, "DATE")
            {

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    int date = inputs.get(0).getInt32();
                    long ymd[] = MDatetimes.decodeDate(date);

                    if (!MDatetimes.isValidDatetime(ymd))
                    {
                        context.reportBadValue("Invalid DATE value " + date);
                        output.putNull();
                    }
                    else
                        output.putInt32(date);
                }
            },
            new MExtract(MDatetimes.TIME, MDatetimes.DATE, "DATE", true)
            {

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    output.putInt32(0);
                }
            },
            new MExtract(MDatetimes.DATETIME, "TIMESTAMP")
            {
                @Override
                public String[] registeredNames()
                {
                    return new String[] {"timestamp", "datetime"};
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    long datetime = inputs.get(0).getInt64();
                    long ymd[] = MDatetimes.decodeDatetime(datetime);

                    if (!MDatetimes.isValidDatetime(ymd))
                    {
                        context.reportBadValue("Invalid DATETIME value " + datetime);
                        output.putNull();
                    }
                    else
                        output.putInt64(datetime);
                }
            },
            new MExtract(MDatetimes.TIME, MDatetimes.DATETIME, "DATETIME", true)
            {
                @Override
                public String[] registeredNames()
                {
                    return new String[] {"timestamp", "datetime"};
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    int time = inputs.get(0).getInt32();
                    long ymdHMS[] = MDatetimes.decodeTime(time);
                    Arrays.fill(ymdHMS, 0, 3, 0); // zero ymd
                    output.putInt64(MDatetimes.encodeDatetime(ymdHMS));
                }
            },
            new MExtract(MDatetimes.TIME, "TIME")
            {
                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
                {
                    int time = inputs.get(0).getInt32();
                    long hms[] = MDatetimes.decodeTime(time);

                    if (!MDatetimes.isValidHrMinSec(hms, false))
                    {
                        context.reportBadValue("Invalid TIME value: " + time);
                        output.putNull();
                    }
                    else
                        output.putInt32(time);
                }
            }
        };
    }
    
    private final TClass input, output;
    private final String name;
    private final boolean exact;
    
    private MExtract (TClass type, String name)
    {
        this(type, type, name, false);
    }

    private MExtract (TClass input, TClass output, String name, boolean exact)
    {
        this.input = input;
        this.output = output;
        this.name = name;
        this.exact = exact;
    }

    @Override
    public int[] getPriorities() {
        // The exact one has priority but only works for its particular case.
        return new int[] { exact ? 1 : 2 };
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.setExact(exact).covers(input, 0);
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(output);
    }
}
