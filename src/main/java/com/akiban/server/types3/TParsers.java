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

package com.akiban.server.types3;

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.types3.mcompat.mcasts.CastUtils;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

import java.math.BigDecimal;

public class TParsers
{
    public static final TParser BOOLEAN = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            String s = source.getString();
            boolean result = false;
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t")) {
                result = true;
            }
            else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f")) {
                result = false;
            }
            else {
                // parse source is a string representing a number-ish, where '0' is false, any other integer is true.
                // We're looking for an optional negative, followed by an optional dot, followed by any number of digits,
                // followed by anything. If any of those digits is not 0, the result is true; otherwise it's false.
                boolean negativeAllowed = true;
                boolean periodAllowed = true;
                for (int i = 0, len = s.length(); i < len; ++i) {
                    char c = s.charAt(i);
                    if (negativeAllowed && c == '-') {
                        negativeAllowed = false;
                    }
                    else if (periodAllowed && c == '.') {
                        periodAllowed = false;
                        negativeAllowed = false;
                    }
                    else if (Character.isDigit(c)) {
                        if (c != '0') {
                            result = true;
                            break;
                        }
                    }
                    else {
                        break;
                    }
                }
            }
            target.putBool(result);
        }
    };
    
    public static final TParser TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte) CastUtils.parseInRange(source.getString(),
                                                         CastUtils.MAX_TINYINT, 
                                                         CastUtils.MIN_TINYINT,
                                                         context));
        }
    };
    
        
    public static final TParser UNSIGNED_TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.parseInRange(source.getString(),
                                            CastUtils.MAX_UNSIGNED_TINYINT,
                                            0,
                                            context));
        }
    };
    
    public static final TParser SMALLINT = new TParser()
    {

        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.parseInRange(source.getString(),
                                             CastUtils.MAX_SMALLINT, 
                                             CastUtils.MIN_SMALLINT,
                                             context));
        }
    };

    public static final TParser UNSIGNED_SMALLINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                          CastUtils.MAX_UNSIGNED_SMALLINT, 
                                          0,
                                          context));
        }
    };

    public static final TParser MEDIUMINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                             CastUtils.MAX_MEDINT, 
                                             CastUtils.MIN_MEDINT,
                                             context));
        }
    };

    public static final TParser UNSIGNED_MEDIUMINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                     CastUtils.MAX_UNSIGNED_MEDINT, 
                                     0,
                                     context));
        }
    };

    public static final TParser INT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                           CastUtils.MAX_INT, 
                                           CastUtils.MIN_INT,
                                           context));
        }
    };

    public static final TParser UNSIGNED_INT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                                   CastUtils.MAX_UNSIGNED_INT,
                                                   0,
                                                   context));
        }
    };

    public static final TParser BIGINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                                   CastUtils.MAX_BIGINT, 
                                                   CastUtils.MIN_BIGINT,
                                                   context));
        }
    };

    public static final TParser UNSIGNED_BIGINT = new TParser() {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target) {
            target.putInt64(CastUtils.parseUnsignedLong(source.getString(), context));
        }
    };
    
    public static final TParser FLOAT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putFloat((float)CastUtils.parseDoubleString(source.getString(), context));
        }
        
    };

    public static final TParser DOUBLE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(CastUtils.parseDoubleString(source.getString(), context));
        }
    };
    
    public static final TParser DECIMAL = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            CastUtils.doCastDecimal(context,
                                    CastUtils.parseDecimalString(source.getString(),context),
                                    target);
        }   
    };

    public static final TParser DECIMAL_UNSIGNED = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            MBigDecimalWrapper wrapped = CastUtils.parseDecimalString(source.getString(), context);
            BigDecimal bd = wrapped.asBigDecimal();
            if (BigDecimal.ZERO.compareTo(bd) < 0)
                wrapped.reset();
            CastUtils.doCastDecimal(context, wrapped, target);
        }
    };
    
    public static final TParser DATE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            try
            {
                int ret = MDatetimes.parseDate(source.getString(), context);
                if (ret < 0)
                    target.putNull();
                else
                    target.putInt32(ret);
            }
            catch (InvalidDateFormatException e)
            {
                context.warnClient(e);
                target.putNull();
            }
        }
    };

    public static final TParser DATETIME = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            try
            {
                target.putInt64(MDatetimes.parseDatetime(source.getString()));
            }
             catch (InvalidDateFormatException e)
            {
                context.warnClient(e);
                target.putNull();
            }
        }
    };
    
    public static final TParser TIME = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            try
            {
                target.putInt32(MDatetimes.parseTime(source.getString(), context));
            }
            catch (InvalidDateFormatException e)
            {
                context.warnClient(e);
                target.putNull();
            }
        }
    };

    public static final TParser TIMESTAMP = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.parseTimestamp(source.getString(),
                                                      context.getCurrentTimezone(),
                                                      context));
        }
    };
    
    public static final TParser YEAR = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16(CastUtils.adjustYear(CastUtils.parseInRange(source.getString(),
                                                                        Long.MAX_VALUE,
                                                                        Long.MIN_VALUE,
                                                                        context),
                                                 context));
        }
    };
}
