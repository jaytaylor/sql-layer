
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.*;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import com.google.common.primitives.UnsignedLongs;

public class MNumericCastBase
{
    static class FromDoubleToInt8 extends TCastBase
    {
        public FromDoubleToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.round(Byte.MAX_VALUE, Byte.MIN_VALUE, 
                                                 source.getDouble(), context));
        }
    }
    
        
    static class FromDoubleToUnsignedInt8 extends TCastBase
    {
        public FromDoubleToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.round(Short.MAX_VALUE, 0, 
                                                 source.getDouble(), context));
        }
    }
        
    static class FromDoubleToInt16 extends TCastBase
    {
        public FromDoubleToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.round(Short.MAX_VALUE, Short.MIN_VALUE, 
                                                        source.getDouble(), context));
        }
    }
    
    static class FromDoubleToUnsignedInt16 extends TCastBase
    {
        public FromDoubleToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(source, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.round(Integer.MAX_VALUE, 0,
                                                 source.getDouble(), context));
        }
    }

    static class FromDoubleToInt32 extends TCastBase
    {
        public FromDoubleToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(source, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.round(Integer.MAX_VALUE, Integer.MIN_VALUE,
                                                 source.getDouble(), context));
        }
    }
    
    static class FromDoubleToUnsignedInt32 extends TCastBase
    {
        public FromDoubleToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64((long)CastUtils.round(Long.MAX_VALUE, 0,
                                                  source.getDouble(), context));
        }
    }

    static class FromDoubleToInt64 extends TCastBase
    {
        public FromDoubleToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64((long)CastUtils.round(Long.MAX_VALUE, Long.MIN_VALUE,
                                                  source.getDouble(), context));
        }
    }
    
    static class FromDoubleToDecimal extends TCastBase
    {
        public FromDoubleToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(source, PUnderlying.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt8(), context));
        }
    }
    
    static class FromInt8ToInt16 extends TCastBase
    {
        public FromInt8ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16(source.getInt8());
        }
    }

    static class FromInt8ToUnsignedInt16 extends TCastBase
    {
        public FromInt8ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt8(), context));
        }
    }

    static class FromInt8ToInt32 extends TCastBase
    {
        public FromInt8ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(source.getInt8());
        }
    }
    
    static class FromInt8ToUnsignedInt32 extends TCastBase
    {
        public FromInt8ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt8(), context));
        }
    }
        
    static class FromInt8ToInt64 extends TCastBase
    {
        public FromInt8ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt8());
        }
    }
    
    static class FromInt8ToDouble extends TCastBase
    {
        public FromInt8ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt8());
        }
    }
    
    static class FromInt8ToDecimal extends TCastBase
    {
        public FromInt8ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt8()));
        }
    }
    
    static class FromInt16ToInt8 extends TCastBase
    {
        public FromInt16ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt16(), context));
        }
    }
    
    static class FromInt16ToUnsignedInt8 extends TCastBase
    {
        public FromInt16ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt16(), context));
        }
    }

    static class FromInt16ToInt16 extends TCastBase
    {
        public FromInt16ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16(source.getInt16());
        }    
    }
    
    static class FromInt16ToUnsignedInt16 extends TCastBase
    {
        public FromInt16ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt16(), context));
        }
    }

    static class FromInt16ToInt32 extends TCastBase
    {
        public FromInt16ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(source.getInt16());
        }
    }
    
    static class FromInt16ToUnsignedInt32 extends TCastBase
    {
        public FromInt16ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt16(), context));
        }
    }
        
    static class FromInt16ToInt64 extends TCastBase
    {
        public FromInt16ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt16());
        }
    }
    
    static class FromInt16ToDouble extends TCastBase
    {
        public FromInt16ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt16());
        }
    }
    
    static class FromInt16ToDecimal extends TCastBase
    {
        public FromInt16ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt16()));
        }
    }

    static class FromInt32ToInt8 extends TCastBase
    {
        public FromInt32ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToUnsignedInt8 extends TCastBase
    {
        public FromInt32ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
        
    static class FromInt32ToInt16 extends TCastBase
    {
        public FromInt32ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToUnsignedInt16 extends TCastBase
    {
        public FromInt32ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToInt32 extends TCastBase
    {
        public FromInt32ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(source.getInt32());
        }
    }
    
    static class FromInt32ToUnsignedInt32 extends TCastBase
    {
        public FromInt32ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
        
    static class FromInt32ToInt64 extends TCastBase
    {
        public FromInt32ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt32());
        }
    }
    
    static class FromInt32ToDouble extends TCastBase
    {
        public FromInt32ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt32());
        }
    }
    
    static class FromInt32ToDecimal extends TCastBase
    {
        public FromInt32ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt32()));
        }
    }
    
    static class FromInt64ToInt8 extends TCastBase
    {
        public FromInt64ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_8), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt8 extends TCastBase
    {
        public FromInt64ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt16 extends TCastBase
    {
        public FromInt64ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_16), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt16 extends TCastBase
    {
        public FromInt64ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt32 extends TCastBase
    {
        public FromInt64ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_32), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, Integer.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt32 extends TCastBase
    {
        public FromInt64ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt64 extends TCastBase
    {
        public FromInt64ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_64), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt64());
        }
    }
    
    static class FromInt64ToDouble extends TCastBase
    {
        public FromInt64ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.DOUBLE), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt64());
        }
    }

    static class FromInt64ToDecimal extends TCastBase
    {
        public FromInt64ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                    checkType(target, PUnderlying.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt64()));
        }
    }

    static class FromUInt64ToDecimal extends TCastBase
    {
        public FromUInt64ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                    checkType(target, PUnderlying.BYTES), c);
        }

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            String asString = UnsignedLongs.toString(source.getInt64());
            target.putObject(new MBigDecimalWrapper(asString));
        }
    }
    
    private static TClass checkType (TClass input, PUnderlying expected)
    {
        if (input.underlyingType() != expected)
            throw new AkibanInternalException("Expected " + expected + " but got " + input.underlyingType());
        return input;
    }
}
