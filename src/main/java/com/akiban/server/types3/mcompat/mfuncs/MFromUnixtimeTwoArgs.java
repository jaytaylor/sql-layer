
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.expression.std.DateTimeField;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import java.util.List;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class MFromUnixtimeTwoArgs extends TScalarBase
{
    public static final TScalar INSTANCE = new MFromUnixtimeTwoArgs();
    
    private static final int RET_INDEX = 0;
    private static final int ERROR_INDEX = 1;
    
    private MFromUnixtimeTwoArgs() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.BIGINT, 0).covers(MString.VARCHAR, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String ret = null;
        InvalidOperationException error = null;
            
        if ((error = (InvalidOperationException) context.preptimeObjectAt(ERROR_INDEX)) == null
                && (ret = (String)context.preptimeObjectAt(RET_INDEX)) == null)
        {
            Object objs[] = computeResult(inputs.get(0).getInt64(),
                                          inputs.get(1).getString(),
                                          context.getCurrentTimezone());
            
            ret = (String) objs[RET_INDEX];
            error = (InvalidOperationException) objs[ERROR_INDEX];
        }

        if (ret == null)
        {
            output.putNull();
            context.warnClient(error);
        }
        else
            output.putString(ret, null);
    }

    @Override
    public String displayName()
    {
        return "FROM_UNIXTIME";
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
                    length = formatArg.instance().attribute(StringAttribute.MAX_LENGTH) * 10;
                else
                {
                    PValueSource unixTime = inputs.get(0).value();
                    
                    // if the unix time value is not literal, get the length
                    // from the format string
                    length = computeLength(format.getString());
                    
                    // if the unix time is available, get the actual length
                    if (unixTime != null)
                    {
                        Object prepObjects[] = computeResult(unixTime.getInt64(),
                                                            format.getString(),
                                                            context.getCurrentTimezone());
                        context.set(RET_INDEX, prepObjects[RET_INDEX]);
                        context.set(ERROR_INDEX, prepObjects[ERROR_INDEX]);
                        
                        // get the real length
                        if (prepObjects[RET_INDEX] != null)
                            length = ((String)prepObjects[RET_INDEX]).length();
                    }
                }
                return MString.VARCHAR.instance(length, anyContaminatingNulls(inputs));
            }
        });
    }
    
    private static Object[] computeResult(long unix, String format, String tz)
    {
        String st = null;
        InvalidOperationException error = null;
        
        try
        {
            st = DateTimeField.getFormatted(new MutableDateTime(unix * 1000L, DateTimeZone.forID(tz)),
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
