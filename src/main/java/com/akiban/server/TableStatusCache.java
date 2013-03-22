
package com.akiban.server;

import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.rowdata.RowDef;
import com.persistit.exception.PersistitInterruptedException;

public interface TableStatusCache {
    /**
     * Create a new TableStatus that will later be attached to the given
     * tableID. It will not usable until {@link TableStatus#setRowDef(RowDef)}
     * is called.
     * @param tableID ID of the table.
     * @return Associated TableStatus.
     */
    TableStatus createTableStatus(int tableID);

    /**
     * Retrieve, or create, a new table status for a memory table that will be
     * serviced by the given factory. Unlike statuses returned from the
     * {@link #createTableStatus(int)} method, these are saved by the TableStatusCache.
     * @param tableID ID of the table.
     * @param factory Factory providing rowCount.
     * @return Associated TableStatus;
     */
    TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory);

    /**
     * Clean up any AIS associated state stored by this cache or any of its 
     * TableStatuses. At a minimum, this will set the RowDef of each TableStatus
     * to <code>null</code>.
     */
    void detachAIS();
}
