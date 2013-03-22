
package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

import java.util.Iterator;

public final class ImmutableRow extends AbstractValuesHolderRow
{
    @Deprecated
    public ImmutableRow(RowType rowType, Iterator<? extends ValueSource> initialValues)
    {
        this(rowType, initialValues, null);
    }

    public ImmutableRow(ProjectedRow row)
    {
        this(row.rowType(), row.getValueSources(), row.getPValueSources());
    }
    public ImmutableRow(RowType rowType, Iterator<? extends ValueSource> initialValues, Iterator<? extends PValueSource> initialPValues)
    {
        super(rowType, false, initialValues, initialPValues);
    }
}
