
package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.API.Ordering;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.statistics.IndexStatistics;

public abstract class BasicFactoryBase implements MemoryTableFactory {
    private final TableName sourceTable;
    
    public BasicFactoryBase(TableName sourceTable) {
        this.sourceTable = sourceTable;
    }

    @Override
    public TableName getName() {
        return sourceTable;
    }

    @Override
    public Cursor getIndexCursor(Index index, Session session,
            IndexKeyRange keyRange, Ordering ordering,
            IndexScanSelector scanSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index) {
        throw new UnsupportedOperationException();
    }

    public RowType getRowType(MemoryAdapter adapter) {
        return adapter.schema().userTableRowType(adapter.schema().ais().getUserTable(sourceTable));
    }
    
    public static String boolResult(boolean bool) {
        return bool ? "YES" : "NO";
    }
}
