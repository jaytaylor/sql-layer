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

/**
 * Structure denotes summary information about a table, including row count,
 * uniqueId and auto-increment values. In general there is one TableStatus per
 * RowDef, and each object refers to the other.
 */
public interface TableStatus {
    /** Record that a row has been deleted. */
    void rowDeleted();

    /** Record that a row has been written. */
    void rowWritten();

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

    /**
     * @return RowDef of the associated table.
     */
    RowDef getRowDef();

    void setRowCount(long rowCount);
}
