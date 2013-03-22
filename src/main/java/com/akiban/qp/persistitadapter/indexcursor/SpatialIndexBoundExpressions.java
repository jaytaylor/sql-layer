
package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

class SpatialIndexBoundExpressions implements BoundExpressions
{
    // BoundExpressions interface

    @Override
    public PValueSource pvalue(int position)
    {
        return pValueSources[position];
    }

    @Override
    public ValueSource eval(int position)
    {
        return valueSources[position];
    }

    // SpatialIndexBoundExpressions interface

    public void value(int position, PValueSource valueSource)
    {
        pValueSources[position] = valueSource;
    }

    public void value(int position, ValueSource valueSource)
    {
        valueSources[position] = valueSource;
    }

    public SpatialIndexBoundExpressions(int nFields)
    {
        if (Types3Switch.ON) {
            pValueSources = new PValue[nFields];
        } else {
            valueSources = new ValueHolder[nFields];
        }
    }

    // Object state

    private PValueSource[] pValueSources;
    private ValueSource[] valueSources;
}
