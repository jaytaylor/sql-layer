/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
