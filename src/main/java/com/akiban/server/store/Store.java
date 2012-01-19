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

package com.akiban.server.store;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import java.util.BitSet;
import java.util.Collection;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface Store extends Service<Store> {

    RowDefCache getRowDefCache();

    void writeRow(Session session, RowData rowData) throws PersistitException;

    void writeRowForBulkLoad(Session session, Exchange hEx,
            RowDef rowDef, RowData rowData, int[] ordinals,
            int[] nKeyColumns, FieldDef[] fieldDefs,
            Object[] hKey) throws PersistitException;

    void updateTableStats(Session session, RowDef rowDef,
            long rowCount);

    void deleteRow(Session session, RowData rowData) throws PersistitException;

    void updateRow(Session session, RowData oldRowData,
                   RowData newRowData,
                   ColumnSelector columnSelector) throws PersistitException;

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

    void truncateGroup(Session session, int rowDefId) throws PersistitException;

    void truncateTableStatus(Session session, int rowDefId) throws RollbackException, PersistitException;

    /**
     * Analyze statistical information about a table. Specifically, construct
     * histograms for its indexes.
     * 
     * @param tableId
     * @throws PersistitException 
     * @throws Exception 
     */
    void analyzeTable(Session session, int tableId);
    void analyzeTable(Session session, int tableId, int sampleSize);

    boolean isDeferIndexes();
    void setDeferIndexes(boolean b);
    void flushIndexes(Session session);
    void deleteIndexes(Session session, Collection<? extends Index> indexes);
    void buildAllIndexes(Session session, boolean deferIndexes);
    void buildIndexes(Session session, Collection<? extends Index> indexes, boolean deferIndexes);

    /**
     * Remove all trees, and their contents, associated with the given table.
     * @param session Session
     * @param table Table
     * @throws PersistitException 
     * @throws Exception 
     */
    void removeTrees(Session session, Table table);

    /** Get the underlying {@link PersistitStore}. */
    public PersistitStore getPersistitStore();
}
