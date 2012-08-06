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

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.expression.std.DateTimeField;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class MDateFormat extends TOverloadBase
{
    public static TOverload[] create()
    {
        return new TOverload[]
        {
            new MDateFormat(MDatetimes.DATE)
            {
                @Override
                protected long[] getYMDHMS(PValueSource source)
                {
                    return MDatetimes.decodeDate(source.getInt32());
                }
            },
            new MDateFormat(MDatetimes.DATETIME)
            {
                @Override
                protected long[] getYMDHMS(PValueSource source)
                {
                    return MDatetimes.decodeDatetime(source.getInt64());
                }
            },
            new MDateFormat(MDatetimes.TIME)
            {
                @Override
                protected long[] getYMDHMS(PValueSource source)
                {
                    long ret[] = MDatetimes.decodeTime(source.getInt32());
                    
                    // adjust the date part to 0s
                    ret[MDatetimes.YEAR_INDEX] = 0;
                    ret[MDatetimes.MONTH_INDEX] = 0;
                    ret[MDatetimes.DAY_INDEX] = 0;
                    
                    return ret;
                }
            }
        };
    }
    
    private static final int RET_INDEX = 0;
    private static final int ERROR_INDEX = 1;
    
    protected abstract long[] getYMDHMS(PValueSource source);
    
    private final TClass dateType;
    
    private MDateFormat(TClass dateType)
    {
        this.dateType = dateType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(dateType, 0).covers(MString.VARCHAR, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String ret = null;
        InvalidOperationException error = null;
        
        if ((error = (InvalidOperationException) context.preptimeObjectAt(ERROR_INDEX)) == null
                && (ret = (String)context.preptimeObjectAt(RET_INDEX)) == null)
        {
            Object objs[] = computeResult (getYMDHMS(inputs.get(0)),
                                           inputs.get(1).getString(),
                                           context.getCurrentTimezone());
            ret = (String) objs[RET_INDEX];
            error = (InvalidOperationException) objs[ERROR_INDEX];
            
        }
        
        if (ret != null)
            output.putString(ret, null);
        else
        {
            output.putNull();
            context.warnClient(error);
        }
    }

    @Override
    public String displayName()
    {
        return "DATE_FORMAT";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue formatArg = inputs.get(1);
                
                PValueSource format = formatArg.value();
                
                int length;
                
                // format is not literal
                // the length is format's precision * 10
                if (format == null)
                    length = formatArg.instance().attribute(StringAttribute.LENGTH) * 10;
                else
                {
                    PValueSource date = inputs.get(0).value();
                    
                    // if the date string is not literal, get the length
                    // from the format string
                    length = computeLength(format.getString());
                    
                    // if date is literal, get the actual length
                    if (date != null)
                    {
                        Object prepObjects[] = computeResult(getYMDHMS(date), format.getString(), context.getCurrentTimezone());
                        context.set(RET_INDEX, prepObjects[RET_INDEX]);
                        context.set(ERROR_INDEX, prepObjects[ERROR_INDEX]);
                        
                        // get the real length
                        if (prepObjects[RET_INDEX] != null)
                            length = ((String)prepObjects[RET_INDEX]).length();
                    }
                }
                return MString.VARCHAR.instance(length);
            }
        });
    }
    
    private static Object[] computeResult(long ymd[], String format, String tz)
    {
        String st = null;
        InvalidOperationException error = null;
        if (!MDatetimes.isValidDatetime(ymd))
            error = new InvalidParameterValueException("Incorrect datetime value");
        else
            try
            {
                st = DateTimeField.getFormatted(MDatetimes.toJodaDatetime(ymd, tz),
                                                format);
            }
            catch (InvalidParameterValueException e)
            {
                st = null;
                error = e;
            }

        return new Object[]
        {
            st,
            error
        };
    }
    private static int computeLength(String st)
    {
        // TODO: parse the format string ONLY to compute the lenght
        // for now, assume it's the number of format specifier * 5
        return st.length() * 5 / 2;
    }
}
