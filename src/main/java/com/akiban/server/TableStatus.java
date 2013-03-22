
package com.akiban.server;

import com.akiban.server.rowdata.RowDef;
import com.persistit.exception.PersistitInterruptedException;

/**
 * Structure denotes summary information about a table, including row count,
 * uniqueId and auto-increment values. In general there is one TableStatus per
 * RowDef, and each object refers to the other.
 */
public interface TableStatus {
    /** Record that a row has been deleted. */
    void rowDeleted();

    /** Record that a row has been written.
     * @param count*/
    void rowsWritten(long count);

    /** Reset, but do not remove, the state of a table. */
    void truncate() throws PersistitInterruptedException;

    /** Set the auto-increment value of a given table. */
    void setAutoIncrement(long value) throws PersistitInterruptedException;

    /** Set the RowDef of a given table.*/
    void setRowDef(RowDef rowDef);

    /** Create a brand new, unique ID for the given table. */
    long createNewUniqueID() throws PersistitInterruptedException;

    /** Set the ordinal value of a given table. */
    void setOrdinal(int value) throws PersistitInterruptedException;

    /**
     * @return Current auto-increment value of the assocated table.
     */
    long getAutoIncrement() throws PersistitInterruptedException;

    /**
     * @return Ordinal of the associated table.
     */
    int getOrdinal() throws PersistitInterruptedException;

    /**
     * @return Current number of rows in the associated table.
     */
    long getRowCount() throws PersistitInterruptedException;

    /**
     * @return Approximate number of rows in the associated table.
     */
    long getApproximateRowCount();

    /**
     * @return The <b>last</b> unique value used for the associated table.
     */
    long getUniqueID() throws PersistitInterruptedException;

    /** @return The table ID this status is for */
    int getTableID();

    void setRowCount(long rowCount);

    long getApproximateUniqueID();

    void setUniqueId(long value);
}
