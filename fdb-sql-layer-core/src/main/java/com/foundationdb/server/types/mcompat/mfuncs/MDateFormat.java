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

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.StringType;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.DateTimeField;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public abstract class MDateFormat extends TScalarBase
{
    public static TScalar[] create()
    {
        return new TScalar[]
        {
            new MDateFormat(MDateAndTime.DATE)
            {
                @Override
                protected long[] getYMDHMS(ValueSource source)
                {
                    long ret[] = MDateAndTime.decodeDate(source.getInt32());
                    return MDateAndTime.isValidDateTime_Zeros(ret) ? ret : null;
                }
            },
            new MDateFormat(MDateAndTime.DATETIME)
            {
                @Override
                protected long[] getYMDHMS(ValueSource source)
                {
                    long ret[] = MDateAndTime.decodeDateTime(source.getInt64());
                    return MDateAndTime.isValidDateTime_Zeros(ret) ? ret : null;
                }
            },
            new MDateFormat(MDateAndTime.TIME)
            {
                @Override
                protected long[] getYMDHMS(ValueSource source)
                {
                    // input cannot be a TIME
                    // explicitly define this so TIMEs wouldn't get cast to other things
                    return null;
                }
            },
            new MDateFormat(MString.VARCHAR)
            {
                @Override
                protected long[] getYMDHMS(ValueSource source)
                {
                    long ret[] = new long[6];
                    StringType strType = MDateAndTime.parseDateOrTime(source.getString(), ret);
                    if(strType == StringType.TIME_ST || !MDateAndTime.isValidType(strType)) {
                        return null;
                    } else {
                        return ret;
                    }
                }
            }
        };
    }
    
    private static final int RET_INDEX = 0;
    private static final int ERROR_INDEX = 1;
    
    protected abstract long[] getYMDHMS(ValueSource source);
    
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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
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
                
                ValueSource format = formatArg.value();
                
                int length;
                
                // format is not literal
                // the length is format's precision * 10
                if (format == null)
                    length = formatArg.type().attribute(StringAttribute.MAX_LENGTH) * 10;
                else
                {
                    ValueSource date = inputs.get(0).value();
                    
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
                return MString.VARCHAR.instance(length, anyContaminatingNulls(inputs));
            }
        });
    }
    
    private static Object[] computeResult(long ymd[], String format, String tz)
    {
        String st = null;
        InvalidOperationException error = null;
        if (ymd == null || MDateAndTime.isZeroDayMonth(ymd))
            error = new InvalidParameterValueException("Incorrect datetime value");
        else
            try
            {
                st = DateTimeField.getFormatted(MDateAndTime.toJodaDateTime(ymd, tz),
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
