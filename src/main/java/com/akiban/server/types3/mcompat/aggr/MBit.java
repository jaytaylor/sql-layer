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
import com.akiban.server.types3.TFixedTypeAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public abstract class MBit extends TFixedTypeAggregator {
    
    public static final TAggregator[] INSTANCES = {
        // BIT_AND
        new MBit("BIT_AND") {

            @Override
            long process(long i0, long i1) {
                return i0 & i1;
            }
        }, 
        // BIT_OR
        new MBit("BIT_OR") {

            @Override
            long process(long i0, long i1) {
                return i0 | i1;
            }
        }, 
        // BIT_XOR
        new MBit("BIT_XOR") {

            @Override
            long process(long i0, long i1) {
                return i0 ^ i1;
            }
        }
    };
    
    abstract long process(long i0, long i1);

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object o) {
        if (!source.isNull()) {
            long incoming = source.getInt64();
            if (!state.hasAnyValue()) {
                state.putInt64(incoming);
            }
            else {
                long previousState = state.getInt64();
                state.putInt64(process(previousState, incoming));
            }
        }    
    }

    @Override
    public void emptyValue(PValueTarget state) {
        state.putNull();
    }

    private MBit(String name) {
        super(name, MNumeric.BIGINT);
    }
}
