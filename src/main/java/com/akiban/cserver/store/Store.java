package com.akiban.cserver.store;

import java.util.List;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.persistit.Exchange;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface Store {

	void startUp() throws Exception;

	void shutDown() throws Exception;

	RowDefCache getRowDefCache();

	boolean isVerbose();

	void setVerbose(final boolean verbose);

	RowCollector getCurrentRowCollector(final int tableId);

	int writeRow(final RowData rowData) throws Exception;

    int writeRowForBulkLoad(final Exchange hEx, final RowDef rowDef, final RowData rowData,
            final int[] ordinals, final FieldDef[][] fieldDefs,
            final Object[][] hKey) throws Exception;

	int deleteRow(final RowData rowData) throws Exception;

	int updateRow(final RowData oldRowData, final RowData newRowData)
			throws Exception;

	long getAutoIncrementValue(final int rowDefId) throws Exception;

	RowCollector newRowCollector(final int rowDefId, final int indexId,
			final int scanFlags, final RowData start, final RowData end,
			final byte[] columnBitMap) throws Exception;

	long getRowCount(final boolean exact, final RowData start,
			final RowData end, final byte[] columnBitMap) throws Exception;

	TableStatistics getTableStatistics(final int tableId) throws Exception;

	int dropTable(final int rowDefId) throws Exception;

	int truncateTable(final int rowDefId) throws Exception;

	int dropSchema(final String schemaName) throws Exception;

	void setOrdinals() throws Exception;

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
	 * @return
	 * @throws Exception
	 */
	List<RowData> fetchRows(final String schemaName, final String tableName,
			final String columnName, final Object least, final Object greatest,
			final String leafTableName) throws Exception;

}
