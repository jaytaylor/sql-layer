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

package com.akiban.qp.row;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;

import java.util.Arrays;

public final class RowValuesHolder {
    // RowValuesHolder interface

    public Object objectAt(int i) {
        return values[i];
    }

    public ValueSource valueSourceAt(int i) {
        return sources[i];
    }

    public RowValuesHolder(Object[] values) {
        this(values, null);
    }

    public RowValuesHolder(Object[] values, AkType[] explicitTypes) {
        this.values = new Object[values.length];
        System.arraycopy(values, 0, this.values, 0, this.values.length);
        this.sources = createSources(this.values, explicitTypes);
    }

    // Object interface

    @Override
    public String toString() {
        return Arrays.toString(values);
    }


    // for use in this class

    private static FromObjectValueSource[] createSources(Object[] values, AkType[] explicitTypes) {
        FromObjectValueSource[] sources = new FromObjectValueSource[values.length];
        for (int i=0; i < values.length; ++i) {
            FromObjectValueSource source = new FromObjectValueSource();
            if(explicitTypes == null || explicitTypes[i] == null)
                source.setReflectively(values[i]);
            else
                source.setExplicitly(values[i], explicitTypes[i]);
            sources[i] = source;
        }
        return sources;
    }

    // object state

    private final Object[] values;
    private final FromObjectValueSource[] sources;
}
