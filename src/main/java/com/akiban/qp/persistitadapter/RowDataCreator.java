
package com.akiban.qp.persistitadapter;

import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.types.AkType;

interface RowDataCreator<S> {
    S eval(RowBase row, int f);
    boolean isNull(S source);
    void put(S source, NewRow into, FieldDef fieldDef, int f);
}
