
package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

public class ValuesRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return valuesHolder.toString();
    }

    // Row interface

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
        return null;
    }

    // ValuesRow interface

    public ValuesRow(RowType rowType, Object... values)
    {
        this.rowType = rowType;
        AkType[] akTypes = new AkType[rowType.nFields()];
        for (int i = 0; i < akTypes.length; i++) {
            akTypes[i] = rowType.typeAt(i);
        }
        this.valuesHolder = new RowValuesHolder(values, akTypes);
    }

    // Object state

    private final RowType rowType;
    private final RowValuesHolder valuesHolder;
}
