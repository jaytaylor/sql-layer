/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
