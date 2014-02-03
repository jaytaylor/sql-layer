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

package com.foundationdb.server.types.mcompat;

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TParser;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.mcompat.mcasts.CastUtils;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.math.BigDecimal;

public class MParsers
{
    public static final TParser TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                                   CastUtils.MAX_BIGINT, 
                                                   CastUtils.MIN_BIGINT,
                                                   context));
        }
    };

    public static final TParser UNSIGNED_BIGINT = new TParser() {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target) {
            target.putInt64(CastUtils.parseUnsignedLong(source.getString(), context));
        }
    };
    
    public static final TParser FLOAT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putFloat((float)CastUtils.parseDoubleString(source.getString(), context));
        }
        
    };

    public static final TParser DOUBLE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putDouble(CastUtils.parseDoubleString(source.getString(), context));
        }
    };
    
    public static final TParser DECIMAL = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            CastUtils.doCastDecimal(context,
                                    CastUtils.parseDecimalString(source.getString(),context),
                                    target);
        }   
    };

    public static final TParser DECIMAL_UNSIGNED = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            BigDecimalWrapperImpl wrapped = CastUtils.parseDecimalString(source.getString(), context);
            BigDecimal bd = wrapped.asBigDecimal();
            if (BigDecimal.ZERO.compareTo(bd) < 0)
                wrapped.reset();
            CastUtils.doCastDecimal(context, wrapped, target);
        }
    };
    
    public static final TParser DATE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            try
            {
                int ret = MDateAndTime.parseAndEncodeDate(source.getString());
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            try
            {
                target.putInt64(MDateAndTime.parseAndEncodeDateTime(source.getString()));
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            try
            {
                target.putInt32(MDateAndTime.parseTime(source.getString(), context));
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
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(MDateAndTime.parseAndEncodeTimestamp(source.getString(),
                                                                 context.getCurrentTimezone(),
                                                                 context));
        }
    };
    
    public static final TParser YEAR = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16(CastUtils.adjustYear(CastUtils.parseInRange(source.getString(),
                                                                        Long.MAX_VALUE,
                                                                        Long.MIN_VALUE,
                                                                        context),
                                                 context));
        }
    };
}
