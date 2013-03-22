
package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TFixedTypeAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public class MGroupConcat extends TFixedTypeAggregator
{
    public static final TAggregator INSTANCE = new MGroupConcat();

    private MGroupConcat() {
        super("group_concat", MString.TEXT);
    }

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object del)
    {
        // skip all NULL rows
        if (source.isNull())
            return;

        // cache a StringBuilder instead?
        state.putString((state.hasAnyValue()
                            ? state.getString() + (String)del
                            : "") 
                            + source.getString(),
                        null);
    }

    @Override
    public void emptyValue(PValueTarget state)
    {
        state.putNull();
    }
}
