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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.*;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

public class MNumericCastBase
{
    static class FromDoubleToInt8 extends TCastBase
    {
        public FromDoubleToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.INT_8), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(target, PUnderlying.INT_8), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(target, PUnderlying.INT_16), auto, c);
        }
        
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(source, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(source, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
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
                  checkType(source, PUnderlying.BYTES), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TODO: determine the target precision and scale then correctly round 
            // the source to that type
            throw new UnsupportedOperationException();
        }
    }

    static class FromDoubleToString extends TCastBase
    {
        public FromDoubleToString(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.DOUBLE),
                  checkType(target, PUnderlying.STRING), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Double.toString(source.getDouble()));
        }
    }

    static class FromInt8ToString extends TCastBase
    {
        public FromInt8ToString(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.STRING), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Byte.toString(source.getInt8()));
        }
    }

    static class FromInt8ToUnsignedInt8 extends TCastBase
    {
        public FromInt8ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt8(), context));
        }
    }
    
    static class FromInt8ToInt16 extends TCastBase
    {
        public FromInt8ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16(source.getInt8());
        }
    }

    static class FromInt8ToUnsignedInt16 extends TCastBase
    {
        public FromInt8ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt8(), context));
        }
    }

    static class FromInt8ToInt32 extends TCastBase
    {
        public FromInt8ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(source.getInt8());
        }
    }
    
    static class FromInt8ToUnsignedInt32 extends TCastBase
    {
        public FromInt8ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt8(), context));
        }
    }
        
    static class FromInt8ToInt64 extends TCastBase
    {
        public FromInt8ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt8());
        }
    }
    
    static class FromInt8ToDouble extends TCastBase
    {
        public FromInt8ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.DOUBLE), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt8());
        }
    }
    
    static class FromInt8ToDecimal extends TCastBase
    {
        public FromInt8ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_8),
                  checkType(target, PUnderlying.BYTES), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt8()));
        }
    }
    
    static class FromInt16ToString extends TCastBase
    {
        public FromInt16ToString(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.STRING), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Short.toString(source.getInt16()));
        }
    }
    
    static class FromInt16ToInt8 extends TCastBase
    {
        public FromInt16ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_8), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt16(), context));
        }
    }
    
    static class FromInt16ToUnsignedInt8 extends TCastBase
    {
        public FromInt16ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt16(), context));
        }
    }

    static class FromInt16ToInt16 extends TCastBase
    {
        public FromInt16ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }
        
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16(source.getInt16());
        }    
    }
    
    static class FromInt16ToUnsignedInt16 extends TCastBase
    {
        public FromInt16ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt16(), context));
        }
    }

    static class FromInt16ToInt32 extends TCastBase
    {
        public FromInt16ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(source.getInt16());
        }
    }
    
    static class FromInt16ToUnsignedInt32 extends TCastBase
    {
        public FromInt16ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt16(), context));
        }
    }
        
    static class FromInt16ToInt64 extends TCastBase
    {
        public FromInt16ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt16());
        }
    }
    
    static class FromInt16ToDouble extends TCastBase
    {
        public FromInt16ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.DOUBLE), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt16());
        }
    }
    
    static class FromInt16ToDecimal extends TCastBase
    {
        public FromInt16ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_16),
                  checkType(target, PUnderlying.BYTES), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt16()));
        }
    }
    
    static class FromInt32ToString extends TCastBase
    {
        public FromInt32ToString(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.STRING), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Integer.toString(source.getInt32()));
        }
    }

    static class FromInt32ToInt8 extends TCastBase
    {
        public FromInt32ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_8), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToUnsignedInt8 extends TCastBase
    {
        public FromInt32ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
        
    static class FromInt32ToInt16 extends TCastBase
    {
        public FromInt32ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToUnsignedInt16 extends TCastBase
    {
        public FromInt32ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
    
    static class FromInt32ToInt32 extends TCastBase
    {
        public FromInt32ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(source.getInt32());
        }
    }
    
    static class FromInt32ToUnsignedInt32 extends TCastBase
    {
        public FromInt32ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt32(), context));
        }
    }
        
    static class FromInt32ToInt64 extends TCastBase
    {
        public FromInt32ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt32());
        }
    }
    
    static class FromInt32ToDouble extends TCastBase
    {
        public FromInt32ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.DOUBLE), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt32());
        }
    }
    
    static class FromInt32ToDecimal extends TCastBase
    {
        public FromInt32ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_32),
                  checkType(target, PUnderlying.BYTES), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt32()));
        }
    }
    
    static class FromInt64ToString extends TCastBase
    {
        public FromInt64ToString(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.STRING), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Long.toString(source.getInt64()));
        }
    }
    
    static class FromInt64ToInt8 extends TCastBase
    {
        public FromInt64ToInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_8), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt8 extends TCastBase
    {
        public FromInt64ToUnsignedInt8(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt16 extends TCastBase
    {
        public FromInt64ToInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_16), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt16 extends TCastBase
    {
        public FromInt64ToUnsignedInt16(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt32 extends TCastBase
    {
        public FromInt64ToInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_32), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, Integer.MIN_VALUE, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToUnsignedInt32 extends TCastBase
    {
        public FromInt64ToUnsignedInt32(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, source.getInt64(), context));
        }
    }
    
    static class FromInt64ToInt64 extends TCastBase
    {
        public FromInt64ToInt64(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.INT_64), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(source.getInt64());
        }
    }
    
    static class FromInt64ToDouble extends TCastBase
    {
        public FromInt64ToDouble(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.DOUBLE), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt64());
        }
    }
    
    static class FromInt64ToDecimal extends TCastBase
    {
        public FromInt64ToDecimal(TClass source, TClass target, boolean auto, Constantness c)
        {
            super(checkType(source, PUnderlying.INT_64),
                  checkType(target, PUnderlying.BYTES), auto, c);
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt64()));
        }
    }
    
    private static TClass checkType (TClass input, PUnderlying expected)
    {
        if (input.underlyingType() != expected)
            throw new AkibanInternalException("Expected " + expected + " but got " + input.underlyingType());
        return input;
    }
}
