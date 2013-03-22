
package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;

import java.util.Arrays;

public final class PValuesRow extends AbstractRow {
    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public PValueSource pvalue(int i) {
        return values[i];
    }

    @Override
    public HKey hKey() {
        return null;
    }

    public PValuesRow(RowType rowType, PValueSource... values) {
        this.rowType = rowType;
        this.values = values;
        if (rowType.nFields() != values.length) {
            throw new IllegalArgumentException(
                    "row type " + rowType + " requires " + rowType.nFields() + " fields, but "
                            + values.length + " values given: " + Arrays.asList(values));
        }
        for (int i = 0, max = values.length; i < max; ++i) {
            TClass requiredType = rowType.typeInstanceAt(i).typeClass();
            TClass actualType = TInstance.tClass(values[i].tInstance());
            if (requiredType != actualType)
                throw new IllegalArgumentException("value " + i + " should be " + requiredType
                        + " but was " + actualType);
        }
    }

    private final RowType rowType;
    private final PValueSource[] values;
}
