
package com.akiban.qp.persistitadapter;

import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;

final class OldRowDataCreator implements RowDataCreator<ValueSource> {

    @Override
    public ValueSource eval(RowBase row, int f) {
        return row.eval(f);
    }

    @Override
    public boolean isNull(ValueSource source) {
        return source.isNull();
    }

    @Override
    public void put(ValueSource source, NewRow into, FieldDef fieldDef, int f) {
        into.put(f, target.convertFromSource(source));
    }

    private ToObjectValueTarget target = new ToObjectValueTarget();
}
