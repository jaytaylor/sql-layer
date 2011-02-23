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

    RowCollector newRowCollector(final Session session, final int rowDefId,
            final int indexId, final int scanFlags, final RowData start,
            final RowData end, final byte[] columnBitMap, boolean messageOutput) throws Exception;

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

    void truncateTable(final Session session, final int rowDefId)
            throws Exception;

    /**
     * <p>
     * Fetch and return a List of RowData objects. This is a somewhat
     * generalized interface to the core row-scanning logic suitable for
     * object-level access by MemcacheD, for example. This method may return
     * rows of multiple types, depending on the parameters. For example, this
     * method may be used to request a Customer by customer_id, along with all
     * that customer's orders and their items.
     * </p>
     * 
     * <p>
     * The method supports selection from an index based on a range of values
     * for one designated column. There must be an index on the specified
     * column. Typically one would specify a primary key column and the PK index
     * would be used, but any column with an index is permitted. The least and
     * greatest values represent bounds on the column values in rows returned by
     * this method; if least and greatest are the same then this method selects
     * all rows having that value; in the normal case that the index is unique,
     * this will be at most one row. For example, in the COI schema, if
     * columnName="customer" and least and greatest are both equal to 123, then
     * just the customer row for customer_id=123 is returned.
     * </p>
     * 
     * <p>
     * This method also supports selecting descendant rows from the specified
     * table according the the natural join relationships inherent in the group.
     * The leafTableName controls this behavior. Specify:
     * <dl>
     * <dt>leafTableName = tableName</dt>
     * <dd>to restrict the results to a single table</dd>
     * <dt>leafTableName = childTableName</dt>
     * <dd>to return rows from all tables in the group found on a path from
     * tableName to childTableName. For example, in our COI schema specify
     * tableName="customer" and leafTableName="order" to return customer row(s)
     * plus all their contained orders.</dd>
     * <dt>leafTableName = null</dt>
     * <dd>for convenience, this method attempts to find a leaf table and
     * follows the above logic with it. For example, with the COI schema,
     * specifying leafTableName=null is equivalent to specifying
     * leafTableName="item". In a "bushy" schema, like CAOI, the results are
     * indeterminate.</dd>
     * </dl>
     * </p>
     * 
     * <p>
     * The Object types for the least and greatest values should be chosen to
     * match the data type of the column. For numeric columns, use a numeric
     * type (width does not matter: for example, you can specify Integer-valued
     * least and greatest values for a BIGINT column), for date/timestamp/year
     * columns specify java.util.Date, for char/varchar (and in some cases the
     * blob-type columns), specify a String. For most common data types
     * fetchRows can make the natural translation from a Java Object to the
     * MySQL column value.
     * </p>
     * 
     * @param schemaName
     *            schema name
     * @param tableName
     *            table name - should be a user table, such as "Customer"
     * @param columnName
     *            column name on which index values are specified
     * @param least
     *            the low end of the retrieval range, inclusive for the column
     *            specified by columnName.
     * @param greatest
     *            the high end of the retrieval range, inclusive, for the column
     *            specified by columnName.
     * @param leafTableName
     *            optional user table name specifying the leafmost table to
     *            retrieve from, e.g., "item" in the COI schema.
     * @return List of RowData objects containing the result
     * @throws Exception
     */
    List<RowData> fetchRows(final Session session, final String schemaName,
            final String tableName, final String columnName,
            final Object least, final Object greatest,
            final String leafTableName) throws Exception;

    /**
     * Similar to
     * {@link #fetchRows(String, String, String, Object, Object, String)}. This
     * method takes a supplied ByteBuffer rather than allocating one for row
     * scanning.
     * 
     * @param schemaName
     *            schema name
     * @param tableName
     *            table name - should be a user table, such as "Customer"
     * @param columnName
     *            column name on which index values are specified
     * @param least
     *            the low end of the retrieval range, inclusive for the column
     *            specified by columnName.
     * @param greatest
     *            the high end of the retrieval range, inclusive, for the column
     *            specified by columnName.
     * @param leafTableName
     *            optional user table name specifying the leafmost table to
     *            retrieve from, e.g., "item" in the COI schema.
     * @param payload
     *            ByteBuffer to be used in collecting rows
     * @return List of RowData objects containing the result
     * @throws Exception
     */
    List<RowData> fetchRows(final Session session, final String schemaName,
            final String tableName, final String columnName,
            final Object least, final Object greatest,
            final String leafTableName, final ByteBuffer payload)
            throws Exception;

    /**
     * Analyze statistical information about a table. Specifically, construct
     * histograms for its indexes.
     * 
     * @param tableId
     * @throws Exception
     */
    void analyzeTable(final Session session, int tableId) throws Exception;

    /**
     * Register a CommittedUpdateListener to handle update events.
     * 
     * @param listener
     *            The listener to add
     */
    void addCommittedUpdateListener(final CommittedUpdateListener listener);

    /**
     * Remove a CommitedUpdateListener.
     * 
     * @param listener
     *            The listener to remove
     */
    void removeCommittedUpdateListener(final CommittedUpdateListener listener);

    boolean isDeferIndexes();

    void setDeferIndexes(final boolean b);
    
    // TODO - temporary - we want this to be a separate service acquired
    // from ServiceManager.
    IndexManager getIndexManager();

    void deleteIndexes(Session session, String string);

    void buildIndexes(Session session, String string);
    
    void flushIndexes(Session session);
}
