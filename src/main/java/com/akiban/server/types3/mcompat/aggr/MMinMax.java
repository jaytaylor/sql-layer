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
import com.akiban.server.types3.TAggregatorBase;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;

public class MMinMax extends TAggregatorBase {

    private MType mType;
    
    private enum MType {
        MIN() {
            @Override
            boolean condition(int a) {
                return a < 0;
            }   
        }, 
        MAX() {
            @Override
            boolean condition(int a) {
                return a > 0;
            }
        };
        abstract boolean condition (int a);
    }

    public static final TAggregator MIN = new MMinMax(MType.MIN);
    public static final TAggregator MAX = new MMinMax(MType.MAX);
    
    private MMinMax(MType mType) {
        this.mType = mType;
    }

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
        if (source.isNull())
            return;
        if (!state.hasAnyValue()) {
            PValueTargets.copyFrom(source, state);
            return;
        }
        TClass tClass = instance.typeClass();
        assert stateType.typeClass().equals(tClass) : "incompatible types " + instance + " and " + stateType;
        int comparison = TClass.compare(instance, source, stateType, state);
        if (mType.condition(comparison))
            PValueTargets.copyFrom(source, state);
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
        return null;
    }

    @Override
    public String name() {
        return mType.name();
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
