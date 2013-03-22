
package com.akiban.server.test.it.qp;

import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.RowValuesHolder;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;

public class TestRow extends AbstractRow
{
    // RowBase interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return valuesHolder.valueSourceAt(i);
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // TestRow interface

    public TestRow(RowType rowType, Object[] fields, String hKeyString)
    {
        this(rowType, new RowValuesHolder(fields), hKeyString);
    }

    public TestRow(RowType rowType, Object[] fields) {
        this(rowType, fields, null);
    }

    public TestRow(RowType rowType, RowValuesHolder valuesHolder, String hKeyString) {
        this.rowType = rowType;
        this.valuesHolder = valuesHolder;
        this.hKeyString = hKeyString;
    }

    public String persistityString() {
        return hKeyString;
    }

    // Object state

    private final RowType rowType;
    private final RowValuesHolder valuesHolder;
    private final String hKeyString;
}
