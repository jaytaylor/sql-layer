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
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.RowDefCache;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

import java.util.Collection;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface Store extends Service<Store> {

    RowDefCache getRowDefCache();

    void writeRow(final Session session, final RowData rowData)
            throws Exception;

    void writeRowForBulkLoad(final Session session, final Exchange hEx,
            final RowDef rowDef, final RowData rowData, final int[] ordinals,
            final int[] nKeyColumns, final FieldDef[] fieldDefs,
            final Object[] hKey) throws Exception;

    void updateTableStats(final Session session, final RowDef rowDef,
            long rowCount) throws Exception;

    void deleteRow(final Session session, final RowData rowData)
            throws Exception;

    void updateRow(final Session session, final RowData oldRowData,
                   final RowData newRowData,
                   final ColumnSelector columnSelector) throws Exception;

    /**
     * See {@link #newRowCollector(Session, int, int, int, byte[], RowData, ColumnSelector, RowData, ColumnSelector, ScanLimit)}
     * for parameter descriptions.
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
                                 ScanLimit scanLimit) throws Exception;

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
                                 ScanLimit scanLimit) throws Exception;
    /**
     * Get the previously saved RowCollector for the specified tableId. Used in
     * processing the ScanRowsMoreRequest message.
     * 
     * @param tableId
     * @return
     */
    RowCollector getSavedRowCollector(final Session session, final int tableId)
            throws InvalidOperationException;

    /**
     * Push a RowCollector onto a stack so that it can subsequently be
     * referenced by getSavedRowCollector.
     * 
     * @param rc
     */
    void addSavedRowCollector(final Session session, final RowCollector rc);

    /***
     * Remove a previously saved RowCollector. Must the the most recently added
     * RowCollector for a table.
     * 
     * @param rc
     */
    void removeSavedRowCollector(final Session session, final RowCollector rc)
            throws InvalidOperationException;

    long getRowCount(final Session session, final boolean exact,
            final RowData start, final RowData end, final byte[] columnBitMap)
            throws Exception;

    TableStatistics getTableStatistics(final Session session, final int tableId)
            throws Exception;

    void truncateGroup(final Session session, final int rowDefId)
            throws Exception;

    void truncateTableStatus(Session session, int rowDefId)
        throws PersistitException;

    /**
     * Analyze statistical information about a table. Specifically, construct
     * histograms for its indexes.
     * 
     * @param tableId
     * @throws Exception
     */
    void analyzeTable(final Session session, int tableId) throws Exception;
    void analyzeTable(final Session session, int tableId, int sampleSize) throws Exception;

    boolean isDeferIndexes();
    void setDeferIndexes(final boolean b);
    void flushIndexes(Session session) throws Exception;
    void deleteIndexes(Session session, Collection<? extends Index> indexes) throws Exception;
    void buildAllIndexes(Session session, boolean deferIndexes) throws Exception;
    void buildIndexes(Session session, Collection<? extends Index> indexes, boolean deferIndexes) throws Exception;

    /**
     * Remove all trees, and their contents, associated with the given table.
     * @param session Session
     * @param table Table
     * @throws Exception For any error.
     */
    void removeTrees(Session session, Table table) throws Exception;
}
