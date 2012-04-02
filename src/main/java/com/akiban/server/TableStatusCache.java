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

import com.akiban.server.rowdata.RowDef;
import com.persistit.exception.PersistitInterruptedException;

public interface TableStatusCache {
    /**
     * Record that a row has been deleted.
     * @param tableID ID of the modified table.
     */
    void rowDeleted(int tableID);

    /**
     * Record that a row has been written.
     * @param tableID ID of the modified table.
     */
    void rowWritten(int tableID);

    /**
     * Reset, but do not remove, the state of a table.
     * @param tableID ID of the table to truncate.
     */
    void truncate(int tableID) throws PersistitInterruptedException;

    /**
     * Completely remove the state of a table.
     * @param tableID ID of the table to dop.
     */
    void drop(int tableID) throws PersistitInterruptedException;

    /**
     * Set the auto-increment value of a given table.
     * @param tableID ID of the table.
     * @param value The new auto-increment value.
     */
    void setAutoIncrement(int tableID, long value) throws PersistitInterruptedException;

    /**
     * Set the RowDef of a given table.
     * @param tableID ID of the table.
     * @param rowDef Associated RowDef.
     */
    void setRowDef(int tableID, RowDef rowDef);

    /**
     * Create a brand new, unique ID for the given table.
     * @param tableID ID of the table.
     * @return The new ID value.
     */
    long createNewUniqueID(int tableID) throws PersistitInterruptedException;

    /**
     * Set the ordinal value of a given table.
     * @param tableID ID of the table.
     * @param value Value to set the ordinal to.
     */
    void setOrdinal(int tableID, int value) throws PersistitInterruptedException;

    /**
     * Retrieve the, read-only, view of the table status for a given table.
     * This method will instantiate a new TableStatus if one does not exist.
     * @param tableID ID of the table.
     * @return Associated TableStatus.
     */
    TableStatus getTableStatus(int tableID);

    /**
     * Load the saved state of all TableStatuses associated with the given volume.
     * @param volumeName Name of the volume to load.
     * @throws Exception For any error encountered during the load.
     */
    void loadAllInVolume(String volumeName) throws Exception;

    /**
     * Clean up any AIS associated state stored by this cache or any of its 
     * TableStatuses. At a minimum, this will set the RowDef of each TableStatus
     * to <code>null</code>.
     */
    void detachAIS();
}
