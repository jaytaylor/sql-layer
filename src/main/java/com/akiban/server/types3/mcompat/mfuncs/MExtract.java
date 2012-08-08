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
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * implement TIMESTAMP(<expr>), DATE(<expr>), TIME(<expr>), ... functions
 */
public class MExtract extends TOverloadBase
{
    public static List<? extends TOverload> create ()
    {
        LinkedList<MExtract> list = new LinkedList<MExtract>();

        for (RetType ret : RetType.values())
            for (InputType input : InputType.values())
                list.add(new MExtract(input, ret));
        return list;
    }
    
    private static enum RetType
    {
        TIMESTAMP(MDatetimes.DATETIME)
        {
            @Override
            void putVal(PValueTarget target, long ymd[], TExecutionContext context)
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
        DATE(MDatetimes.DATE)
        {
            @Override
            void putVal(PValueTarget target, long ymd[], TExecutionContext context)
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
        TIME(MDatetimes.TIME)
        {
            @Override
            void putVal(PValueTarget target, long ymd[], TExecutionContext context)
            {
                if (!MDatetimes.isValidDatetime(ymd))
                {
                    context.reportBadValue("Invalid DATETIME value");
                    target.putNull();
                }
                else
                    target.putInt32(MDatetimes.encodeTime(ymd));
            }
        };
        
        final TClass type;
        
        abstract void putVal (PValueTarget target, long ymd[], TExecutionContext context);
        
        private RetType( TClass t)
        {
            type = t;
        }
    }

    private static enum InputType
    {
        DATE(MDatetimes.DATE)
        {
            @Override
            long[] extract (PValueSource source, TExecutionContext context)
            {
                return MDatetimes.decodeDate(source.getInt32());
            }
        },
        DATETIME(MDatetimes.DATETIME)
        {
            @Override
            long[] extract (PValueSource source, TExecutionContext context)
            {
                return MDatetimes.decodeDatetime(source.getInt64());
            }
        },
        TIMESTAMP(MDatetimes.TIMESTAMP)
        {
            @Override
            long[] extract (PValueSource source, TExecutionContext context)
            {
                return MDatetimes.decodeTimestamp(source.getInt32(), context.getCurrentTimezone());
            }
        },
        TIME(MDatetimes.TIME)
        {
            @Override
            long[] extract (PValueSource source, TExecutionContext context)
            {
                long val[] = MDatetimes.decodeTime(source.getInt32());
                
                //adjust YEAR, MONTH, DAY to 0
                val[MDatetimes.YEAR_INDEX] = 0;
                val[MDatetimes.MONTH_INDEX] = 0;
                val[MDatetimes.DAY_INDEX] = 0;
                return val;
            }
        }
        ;
        
        abstract long[] extract (PValueSource source, TExecutionContext context);
        
        final TClass type;
        private InputType(TClass t)
        {
            type = t;
        }
    }

    private final InputType inputType;
    private final RetType retType;
    
    private MExtract (InputType input, RetType ret)
    {
        inputType = input;
        retType = ret;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(inputType.type, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        retType.putVal(output, inputType.extract(inputs.get(0), context), context);
    }

    @Override
    public String displayName()
    {
        return retType.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(retType.type.instance());
    }
}
