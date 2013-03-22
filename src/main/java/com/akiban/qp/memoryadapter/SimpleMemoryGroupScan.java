
package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.TableName;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;

import java.util.Iterator;

public abstract class SimpleMemoryGroupScan<T> implements MemoryGroupCursor.GroupScan {

    protected abstract Object[] createRow(T data, int hiddenPk);

    @Override
    public Row next() {
        if (!iterator.hasNext())
            return null;
        Object[] rowContents = createRow(iterator.next(), ++hiddenPk);
        return new ValuesRow(rowType, rowContents);
    }

    @Override
    public void close() {
        // nothing
    }

    public SimpleMemoryGroupScan(MemoryAdapter adapter, TableName tableName, Iterator<? extends T> iterator) {
        this.iterator = iterator;
        this.rowType = adapter.schema().userTableRowType(adapter.schema().ais().getUserTable(tableName));
    }

    private final Iterator<? extends T> iterator;
    private final RowType rowType;
    private int hiddenPk = 0;
}
