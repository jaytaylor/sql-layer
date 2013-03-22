
package com.akiban.server.test.it.qp;

import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

public final class NullsRow extends AbstractRow {
    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueSource eval(int index) {
        return NullValueSource.only();
    }

    @Override
    public PValueSource pvalue(int index) {
        return PValueSources.getNullSource(rowType.typeInstanceAt(index));
    }

    public NullsRow(RowType rowType) {
        this.rowType = rowType;
    }

    private final RowType rowType;
}
