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
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

/**
 * 
 * implement TIMESTAMP(<expr>), DATE(<expr>), TIME(<expr>), ... functions
 */
public abstract class MExtract extends TOverloadBase
{
    public static TOverload[] create()
    {
        return new TOverload[]
        {
            new MExtract(MDatetimes.DATE, "DATE")
            {
                @Override
                protected void putVal(long[] ymd, PValueTarget target, TExecutionContext context)
                {
                    if (!MDatetimes.isValidDatetime(ymd))
                    {
                        context.reportBadValue("Invalid DATETIME value");
                        target.putNull();
                    }
                    else
                        target.putInt32(MDatetimes.encodeDate(ymd));
                }
            },
            new MExtract(MDatetimes.DATETIME, "TIMESTAMP")
            {
                @Override
                protected void putVal(long[] ymd, PValueTarget target, TExecutionContext context)
                {
                    if (!MDatetimes.isValidDatetime(ymd))
                    {
                        context.reportBadValue("Invalid DATETIME value");
                        target.putNull();
                    }
                    else
                        target.putInt64(MDatetimes.encodeDatetime(ymd));
                }
            },
            new MExtract(MDatetimes.TIME, "TIME")
            {
                @Override
                protected void putVal(long[] ymd, PValueTarget target, TExecutionContext context)
                {
                    if (!MDatetimes.isValidDatetime(ymd))
                    {
                        context.reportBadValue("Invalid DATETIME value");
                        target.putNull();
                    }
                    else
                        target.putInt32(MDatetimes.encodeTime(ymd));
                }
            }
        };
    }
    protected abstract void putVal(long ymd[], PValueTarget target, TExecutionContext context);
    
    private TClass retType;
    private String name;
    
    private MExtract (TClass ret, String name)
    {
        retType = ret;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MDatetimes.DATETIME, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        putVal(MDatetimes.decodeDatetime(inputs.get(0).getInt64()),
               output,
               context);
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(retType.instance());
    }
}
