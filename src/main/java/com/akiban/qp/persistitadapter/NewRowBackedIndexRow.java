
package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;

public class NewRowBackedIndexRow implements RowBase
{
    // Object interface

    @Override
    public String toString()
    {
        return row.toString();
    }

    // NewRowBackedIndexRow interface

    NewRowBackedIndexRow(RowType rowType, NewRow row, TableIndex index) {
        this.rowType = rowType;
        this.row = row;
        this.index = index;
        this.sources = new FromObjectValueSource[rowType.nFields()];
        for (int f = 0; f < rowType.nFields(); f++) {
            this.sources[f] = new FromObjectValueSource();
        }
    }

    // RowBase interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        FieldDef fieldDef = index.getAllColumns().get(i).getColumn().getFieldDef();
        int fieldPos = fieldDef.getFieldIndex();
        FromObjectValueSource source = sources[fieldPos];
        if (row.isColumnNull(fieldPos)) {
            source.setNull();
        } else {
            source.setReflectively(row.get(fieldPos));
        }
        return source;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HKey ancestorHKey(UserTable table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean ancestorOf(RowBase that) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PValueSource pvalue(int i) {
        FieldDef fieldDef = index.getAllColumns().get(i).getColumn().getFieldDef();
        int fieldPos = fieldDef.getFieldIndex();
        FromObjectValueSource source = sources[fieldPos];
        if (row.isColumnNull(fieldPos)) {
            source.setNull();
        } else {
            source.setReflectively(row.get(fieldPos));
        }
        return PValueSources.fromValueSource(source, rowType.typeInstanceAt(fieldPos));
    }

    @Override
    public int compareTo(RowBase row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
    }

    // Object state

    private final NewRow row;
    private final RowType rowType;
    private final TableIndex index;
    private final FromObjectValueSource[] sources;
}
