package com.akiban.cserver.store;

import java.util.List;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;

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

	RowCollector getCurrentRowCollector();

	int writeRow(final RowData rowData) throws Exception;

	int deleteRow(final RowData rowData) throws Exception;

	int updateRow(final RowData oldRowData, final RowData newRowData)
			throws Exception;

	long getAutoIncrementValue(final int rowDefId) throws Exception;

	RowCollector newRowCollector(final int indexId, final RowData start,
			final RowData end, final byte[] columnBitMap) throws Exception;

	long getRowCount(final boolean exact, final RowData start,
			final RowData end, final byte[] columnBitMap) throws Exception;
	
	TableStatistics getTableStatistics(final int tableId) throws Exception;

	int dropTable(final int rowDefId) throws Exception;

	int truncateTable(final int rowDefId) throws Exception;

	int dropSchema(final String schemaName) throws Exception;

	void setOrdinals() throws Exception;

	List<RowData> fetchRows(final String schemaName, final String tableName,
			final String columnName, final Object least, final Object greatest)
			throws Exception;

}
