
package com.akiban.qp.memoryadapter;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.statistics.IndexStatistics;

import static com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;

public interface MemoryTableFactory {
    public TableName getName();
    
    // Used by MemoryAdapter to get cursors
    public GroupScan getGroupScan(MemoryAdapter adapter);

    public Cursor getIndexCursor(Index index, Session session,  IndexKeyRange keyRange,
                                 API.Ordering ordering, IndexScanSelector scanSelector);
    
    // Used by IndexStatistics to compute index statistics
    public long rowCount();
    
    // This should return null for all indexes
    // TODO: describe index implementation on memory tables. 
    public IndexStatistics computeIndexStatistics(Session session, Index index);
}
