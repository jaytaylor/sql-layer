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

package com.akiban.server.store;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.Table;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeLink;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import java.util.Collection;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface Store {

    RowDefCache getRowDefCache();

    void writeRow(Session session, RowData rowData) throws PersistitException;

    void deleteRow(Session session, RowData rowData) throws PersistitException;
    void deleteRow(Session session, RowData rowData, boolean deleteIndexes) throws PersistitException;

    void updateRow(Session session, RowData oldRowData,
                   RowData newRowData,
                   ColumnSelector columnSelector, Index[] indexes) throws PersistitException;

    /**
     * See {@link #newRowCollector(Session, int, int, int, byte[], RowData, ColumnSelector, RowData, ColumnSelector, ScanLimit)}
     * for parameter descriptions.
     * @throws Exception 
     *
     * @deprecated This constructor is ambiguous and may not return the expected rows. Fields from <code>start</code>
     * and <code>end</code> that are <code>NULL</code> are considered to be <b>unset</b>.
     */
    RowCollector newRowCollector(Session session,
                                 int rowDefId,
                                 int indexId,
                                 int scanFlags,
                                 RowData start,
                                 RowData end,
                                 byte[] columnBitMap,
                                 ScanLimit scanLimit);

    /**
     * Create a new RowCollector.
     * 
     * @param session Session to use.
     * @param scanFlags Flags specifying collection parameters (see flags in {@link RowCollector})
     * @param rowDefId ID specifying the type of row to that will be collected.
     * @param indexId The indexId from the given rowDef to collect on or 0 for table scan
     * @param columnBitMap
     * @param start RowData containing values to begin the scan from.
     * @param startColumns ColumnSelector indicating which fields are set in <code>start</code>
     * @param end RowData containing values to stop the scan at.
     * @param endColumns ColumnSelector indicating which fields are set in <code>end</code>
     * @throws Exception 
     */
    RowCollector newRowCollector(Session session,
                                 int scanFlags,
                                 int rowDefId,
                                 int indexId,
                                 byte[] columnBitMap,
                                 RowData start,
                                 ColumnSelector startColumns,
                                 RowData end,
                                 ColumnSelector endColumns,
                                 ScanLimit scanLimit);
    /**
     * Get the previously saved RowCollector for the specified tableId. Used in
     * processing the ScanRowsMoreRequest message.
     * 
     * @param tableId
     * @return
     */
    RowCollector getSavedRowCollector(Session session, int tableId);


    /**
     * Push a RowCollector onto a stack so that it can subsequently be
     * referenced by getSavedRowCollector.
     * 
     * @param rc
     */
    void addSavedRowCollector(Session session, RowCollector rc);

    /***
     * Remove a previously saved RowCollector. Must the the most recently added
     * RowCollector for a table.
     * 
     * @param rc
     */
    void removeSavedRowCollector(Session session, RowCollector rc);

    long getRowCount(Session session, boolean exact,
            RowData start, RowData end, byte[] columnBitMap);

    TableStatistics getTableStatistics(Session session, int tableId);

    /**
     * Delete all data associated with the group. This includes
     * all indexes from all tables, group indexes, and the group itself.
     */
    void dropGroup(Session session, Group group) throws PersistitException;

    /**
     * Truncate the given group. This includes indexes from all tables, group
     * indexes, the group itself, and all table statuses.
     */
    void truncateGroup(Session session, Group group) throws PersistitException;

    void truncateTableStatus(Session session, int rowDefId) throws RollbackException, PersistitException;

    boolean isDeferIndexes();
    void setDeferIndexes(boolean b);
    void flushIndexes(Session session);
    void deleteIndexes(Session session, Collection<? extends Index> indexes);
    void buildIndexes(Session session, Collection<? extends Index> indexes, boolean deferIndexes);

    void deleteSequences (Session session, Collection<? extends Sequence> sequences);
    /**
     * Remove all trees, and their contents, associated with the given table.
     * @param session Session
     * @param table Table
     * @throws PersistitException 
     * @throws Exception 
     */
    void removeTrees(Session session, Table table);

    /**
     * Low level operation. Removes the given trees and <i>only</i> the given trees.
     * To ensure metadata and other state is updated, check if another method for
     * specific entities is more appropriate (e.g. {@link #deleteIndexes(Session, Collection)}).
     */
    void removeTrees(Session session, Collection<? extends TreeLink> treeLinks);

    /** Get the underlying {@link PersistitStore}. */
    public PersistitStore getPersistitStore();

    void truncateIndexes(Session session, Collection<? extends Index> indexes);
}
