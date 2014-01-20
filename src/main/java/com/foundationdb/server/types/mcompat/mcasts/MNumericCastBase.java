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

package com.foundationdb.server.types.mcompat.mcasts;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;
import com.google.common.primitives.UnsignedLongs;

public class MNumericCastBase
{
    static class FromDoubleToInt8 extends TCastBase
    {
        public FromDoubleToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(target, UnderlyingType.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt8((byte)CastUtils.round(Byte.MAX_VALUE, Byte.MIN_VALUE, 
                                                 source.getDouble(), context));
        }
    }
    
        
    static class FromDoubleToUnsignedInt8 extends TCastBase
    {
        public FromDoubleToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(target, UnderlyingType.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.round(Short.MAX_VALUE, 0, 
                                                 source.getDouble(), context));
        }
    }
        
    static class FromDoubleToInt16 extends TCastBase
    {
        public FromDoubleToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.round(Short.MAX_VALUE, Short.MIN_VALUE, 
                                                        source.getDouble(), context));
        }
    }
    
    static class FromDoubleToUnsignedInt16 extends TCastBase
    {
        public FromDoubleToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(source, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.round(Integer.MAX_VALUE, 0,
                                                 source.getDouble(), context));
        }
    }

    static class FromDoubleToInt32 extends TCastBase
    {
        public FromDoubleToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(source, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.round(Integer.MAX_VALUE, Integer.MIN_VALUE,
                                                 source.getDouble(), context));
        }
    }
    
    static class FromDoubleToUnsignedInt32 extends TCastBase
    {
        public FromDoubleToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64((long)CastUtils.round(Long.MAX_VALUE, 0,
                                                  source.getDouble(), context));
        }
    }

    static class FromDoubleToInt64 extends TCastBase
    {
        public FromDoubleToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64((long)CastUtils.round(Long.MAX_VALUE, Long.MIN_VALUE,
                                                  source.getDouble(), context));
        }
    }
    
    static class FromDoubleToDecimal extends TCastBase
    {
        public FromDoubleToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.DOUBLE),
                  checkType(source, UnderlyingType.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            // TODO: determine the target precision and scale then correctly round 
            // the source to that type
            throw new UnsupportedOperationException();
        }
    }

    static class FromInt8ToUnsignedInt8 extends TCastBase
    {
        public FromInt8ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt8(), context));
        }
    }
    
    static class FromInt8ToInt16 extends TCastBase
    {
        public FromInt8ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16(source.getInt8());
        }
    }

    static class FromInt8ToUnsignedInt16 extends TCastBase
    {
        public FromInt8ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt8(), context));
        }
    }

    static class FromInt8ToInt32 extends TCastBase
    {
        public FromInt8ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(source.getInt8());
        }
    }
    
    static class FromInt8ToUnsignedInt32 extends TCastBase
    {
        public FromInt8ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt8(), context));
        }
    }
        
    static class FromInt8ToInt64 extends TCastBase
    {
        public FromInt8ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(source.getInt8());
        }
    }
    
    static class FromInt8ToDouble extends TCastBase
    {
        public FromInt8ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putDouble(source.getInt8());
        }
    }
    
    static class FromInt8ToDecimal extends TCastBase
    {
        public FromInt8ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_8),
                  checkType(target, UnderlyingType.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putObject(new BigDecimalWrapperImpl(source.getInt8()));
        }
    }
    
    static class FromInt16ToInt8 extends TCastBase
    {
        public FromInt16ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt16(), context));
        }
    }
    
    static class FromInt16ToUnsignedInt8 extends TCastBase
    {
        public FromInt16ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt16(), context));
        }
    }

    static class FromInt16ToInt16 extends TCastBase
    {
        public FromInt16ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16(source.getInt16());
        }    
    }
    
    static class FromInt16ToUnsignedInt16 extends TCastBase
    {
        public FromInt16ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt16(), context));
        }
    }

    static class FromInt16ToInt32 extends TCastBase
    {
        public FromInt16ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(source.getInt16());
        }
    }
    
    static class FromInt16ToUnsignedInt32 extends TCastBase
    {
        public FromInt16ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt16(), context));
        }
    }
        
    static class FromInt16ToInt64 extends TCastBase
    {
        public FromInt16ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(source.getInt16());
        }
    }
    
    static class FromInt16ToDouble extends TCastBase
    {
        public FromInt16ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putDouble(source.getInt16());
        }
    }
    
    static class FromInt16ToDecimal extends TCastBase
    {
        public FromInt16ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_16),
                  checkType(target, UnderlyingType.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putObject(new BigDecimalWrapperImpl(source.getInt16()));
        }
    }

    static class FromInt32ToInt8 extends TCastBase
    {
        public FromInt32ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToUnsignedInt8 extends TCastBase
    {
        public FromInt32ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
        
    static class FromInt32ToInt16 extends TCastBase
    {
        public FromInt32ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToUnsignedInt16 extends TCastBase
    {
        public FromInt32ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToInt32 extends TCastBase
    {
        public FromInt32ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32(source.getInt32());
        }
    }
    
    static class FromInt32ToUnsignedInt32 extends TCastBase
    {
        public FromInt32ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
        
    static class FromInt32ToInt64 extends TCastBase
    {
        public FromInt32ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(source.getInt32());
        }
    }
    
    static class FromInt32ToDouble extends TCastBase
    {
        public FromInt32ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putDouble(source.getInt32());
        }
    }
    
    static class FromInt32ToDecimal extends TCastBase
    {
        public FromInt32ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_32),
                  checkType(target, UnderlyingType.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putObject(new BigDecimalWrapperImpl(source.getInt32()));
        }
    }
    
    static class FromInt64ToInt8 extends TCastBase
    {
        public FromInt64ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt8 extends TCastBase
    {
        public FromInt64ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt16 extends TCastBase
    {
        public FromInt64ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt16 extends TCastBase
    {
        public FromInt64ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt32 extends TCastBase
    {
        public FromInt64ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, Integer.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt32 extends TCastBase
    {
        public FromInt64ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt64 extends TCastBase
    {
        public FromInt64ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putInt64(source.getInt64());
        }
    }
    
    static class FromInt64ToDouble extends TCastBase
    {
        public FromInt64ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                  checkType(target, UnderlyingType.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putDouble(source.getInt64());
        }
    }

    static class FromInt64ToDecimal extends TCastBase
    {
        public FromInt64ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                    checkType(target, UnderlyingType.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            target.putObject(new BigDecimalWrapperImpl(source.getInt64()));
        }
    }

    static class FromUInt64ToDecimal extends TCastBase
    {
        public FromUInt64ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, UnderlyingType.INT_64),
                    checkType(target, UnderlyingType.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target)
        {
            String asString = UnsignedLongs.toString(source.getInt64());
            target.putObject(new BigDecimalWrapperImpl(asString));
        }
    }
    
    private static TClass checkType (TClass input, UnderlyingType expected)
    {
        if (input.underlyingType() != expected)
            throw new AkibanInternalException("Expected " + expected + " but got " + input.underlyingType());
        return input;
    }
}
