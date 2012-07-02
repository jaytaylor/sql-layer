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

import com.akiban.server.error.OverflowException;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import java.math.BigInteger;

public class MSum implements TAggregator {

    private final SumType sumType;
    
    private enum SumType {
        LONG(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                long oldState = source.getInt64();
                long input = state.getInt64();
                long sum = oldState + input;
                if (oldState > 0 && input > 0 && sum <= 0) {
                    throw new OverflowException();
                } else if (oldState < 0 && input < 0 && sum >= 0) {
                    throw new OverflowException();
                } else {
                    state.putInt64(sum);
                }
            }
        }, 
        DOUBLE(MApproximateNumber.DOUBLE) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                double oldState = source.getDouble();
                double input = state.getDouble();
                double sum = oldState + input;
                if (Double.isInfinite(sum) && !Double.isInfinite(oldState) && !Double.isInfinite(input)) {
                    throw new OverflowException();
                } else {
                    state.putDouble(sum);
                }
            }
        }, 
        FLOAT(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                float oldState = source.getFloat();
                float input = state.getFloat();
                float sum = oldState + input;
                if (Float.isInfinite(sum) && !Float.isInfinite(oldState) && !Float.isInfinite(input)) {
                    throw new OverflowException();
                } else {
                    state.putFloat(sum);
                }
            }
        }, 
        BIGDECIMAL(MNumeric.DECIMAL) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                BigDecimalWrapper oldState = (BigDecimalWrapper) source.getObject();
                BigDecimalWrapper input = (BigDecimalWrapper) state.getObject();
                state.putObject(oldState.add(input));
            }
        }, 
        BIGINTEGER(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                BigInteger oldState = (BigInteger) source.getObject();
                BigInteger input = (BigInteger) state.getObject();
                state.putObject(oldState.add(input));
            }            
        };
        abstract void input(TInstance instance, PValueSource source, TInstance stateType, PValue state);
        private final TClass typeClass;
        
        private SumType(TClass typeClass) {
            this.typeClass = typeClass;
        }
    }
    
    public static final TAggregator[] INSTANCES = {
        new MSum(SumType.BIGDECIMAL),
        new MSum(SumType.BIGINTEGER),
        new MSum(SumType.DOUBLE),
        new MSum(SumType.FLOAT), 
        new MSum(SumType.LONG)
    };
    
    private MSum(SumType sumType) {
        this.sumType = sumType;
    }
    
    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
        sumType.input(instance, source, stateType, state);
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
        return sumType.typeClass;
    }

}
