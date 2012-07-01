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

package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import java.math.BigInteger;

public class MMinMax implements TAggregator {

    private static  TType tType;
    private static MType mType;
    
    private enum MType {
        MIN() {
            @Override
            boolean condition(double a) {
                return a < 0;
            }   
        }, 
        MAX() {
            @Override
            boolean condition(double a) {
                return a > 0;
            }
        };
        abstract boolean condition (double a);
    }
    
    private enum TType {
        LONG(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                long oldState = source.getInt64();
                long input = state.getInt64();
                state.putInt64(mType.condition(oldState - input) ? oldState : input);
            } 
        }, 
        DOUBLE(MApproximateNumber.INSTANCE) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                double oldState = source.getDouble();
                double input = state.getDouble();
                state.putDouble(mType.condition(oldState - input) ? oldState : input);
            }
        }, 
        /* TODO: define MDouble.FLOAT
        FLOAT(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                float oldState = source.getFloat();
                float input = state.getFloat();
                state.putFloat(mType.condition(oldState - input) ? oldState : input);
            }
        },*/ 
        BIGDECIMAL(MNumeric.DECIMAL) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                BigDecimalWrapper oldState = (BigDecimalWrapper) source.getObject();
                BigDecimalWrapper input = (BigDecimalWrapper) state.getObject();
                double sign = oldState.subtract(input).getSign();
                state.putObject(mType.condition(sign) ? oldState : input);
            }
        }, 
        BIGINTEGER(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                BigInteger oldState = (BigInteger) source.getObject();
                BigInteger input = (BigInteger) state.getObject();
                double sign = oldState.compareTo(input);
                state.putObject(mType.condition(sign) ? oldState : input);
            }            
        }, 
        STRING(MString.VARCHAR) {
            @Override
            public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                String oldState = (String) source.getObject();
                String input = (String) state.getObject();
                state.putObject(mType.condition(oldState.compareTo(input)) ? oldState : input);
            }            
        }, 
        BOOLEAN(MNumeric.TINYINT) {
             @Override
            public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                 state.putInt8((byte) 1);
             }
        };
        abstract void input(TInstance instance, PValueSource source, TInstance stateType, PValue state);
        private final TClass typeClass;
        
        private TType(TClass typeClass) {
            this.typeClass = typeClass;
        }
    }
    
    public static final TAggregator[] INSTANCES = {
        new MMinMax(MType.MAX, TType.LONG),
        //new MMinMax(MType.MAX, TType.FLOAT),
        new MMinMax(MType.MAX, TType.BIGINTEGER),
        new MMinMax(MType.MAX, TType.BIGDECIMAL),
        new MMinMax(MType.MAX, TType.DOUBLE),
        new MMinMax(MType.MAX, TType.STRING),
        new MMinMax(MType.MAX, TType.BOOLEAN),
        new MMinMax(MType.MIN, TType.LONG),
        //new MMinMax(MType.MIN, TType.FLOAT),
        new MMinMax(MType.MIN, TType.BIGINTEGER),
        new MMinMax(MType.MIN, TType.BIGDECIMAL),
        new MMinMax(MType.MIN, TType.DOUBLE),
        new MMinMax(MType.MIN, TType.STRING),
        new MMinMax(MType.MIN, TType.BOOLEAN)
    };
    
    private MMinMax(MType mType, TType tType) {
        this.mType = mType;
        this.tType = tType;
    }

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
        tType.input(instance, source, stateType, state);
    }

    @Override
    public void emptyValue(PValueTarget state) {
        state.putNull();
    }

    @Override
    public TInstance resultType(TPreptimeValue value) {
        return value.instance();
    }

    @Override
    public TClass getTypeClass() {
        return tType.typeClass;
    }
}
