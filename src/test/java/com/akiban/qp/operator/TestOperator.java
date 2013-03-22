
package com.akiban.qp.operator;

import java.util.ArrayList;
import java.util.Collection;

import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.Types3Switch;


public final class TestOperator extends ValuesScan_Default {

    public TestOperator(RowsBuilder rowsBuilder) {
        super (bindableRows(rowsBuilder.rows()), rowsBuilder.rowType());
    }
    
    public TestOperator (Collection<? extends Row> rows, RowType rowType) {
        super(bindableRows(rows), rowType);
    }

    private static Collection<? extends BindableRow> bindableRows(Collection<? extends Row> rows) {
        Collection<BindableRow> result = new ArrayList<>();
        for (Row row : rows) {
            result.add(BindableRow.of(row, Types3Switch.ON));
        }
        return result;
    }
}
