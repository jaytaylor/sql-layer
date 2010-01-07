package com.akiba.cserver.store;

import com.akiba.cserver.RowData;
import com.akiba.message.AkibaConnection;

/**
 * An abstraction for a layer that stores and retrieves data
 * @author peter
 *
 */
public interface Store {

	public void startUp() throws Exception;
	
	public void shutDown() throws Exception;
	
	public void writeRow(final AkibaConnection connection, final RowData rowData) throws Exception;
}
