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

import java.nio.ByteBuffer;
import java.util.List;

import com.akiban.server.FieldDef;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.RowDefCache;
import com.akiban.server.TableStatistics;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.message.ScanRowsRequest;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface Store extends Service<Store> {

    RowDefCache getRowDefCache();

    boolean isVerbose();

    void setVerbose(final boolean verbose);

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
     * @param scanRowsRequest
     * @return The RowCollector that will generated the requested rows
     */
    RowCollector newRowCollector(final Session session,
            ScanRowsRequest scanRowsRequest) throws Exception;

    /**
     * Version of newRowCollector used in tests and a couple of sites local to
     * akserver. Eliminates having to serialize a ScanRowsRequestt to convey
     * these parameters.
     * 
     * @param rowDefId
     * @param indexId
     * @param scanFlags
     * @param start
     * @param end
     * @param columnBitMap
     * @return
     * @throws Exception
     */
    RowCollector newRowCollector(final Session session, final int rowDefId,
            final int indexId, final int scanFlags, final RowData start,
            final RowData end, final byte[] columnBitMap) throws Exception;

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

    boolean isDeferIndexes();

    void setDeferIndexes(final boolean b);
    
    // TODO - temporary - we want this to be a separate service acquired
    // from ServiceManager.
    IndexManager getIndexManager();

    void deleteIndexes(Session session, String string);

    void buildIndexes(Session session, String string);
    
    void flushIndexes(Session session);

}
