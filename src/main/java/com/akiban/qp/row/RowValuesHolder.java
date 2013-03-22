
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
