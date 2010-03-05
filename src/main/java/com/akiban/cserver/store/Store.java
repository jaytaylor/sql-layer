package com.akiban.cserver.store;

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

	int dropTable(final int rowDefId) throws Exception;
}
