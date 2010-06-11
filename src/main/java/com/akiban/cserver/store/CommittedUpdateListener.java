package com.akiban.cserver.store;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.persistit.KeyState;

/**
 * Listener interface for database update events.  These methods
 * are called from within the Transaction commit logic of 
 * Persistit. In normal operation they must not throw exceptions
 * and should complete quickly without blocking.
 * 
 * @author peter
 *
 */
public interface CommittedUpdateListener {

	/**
	 * Invoked when a newly inserted row has been committed.
	 * @param keyState
	 * @param rowDef
	 * @param rowData
	 */
	void inserted(final KeyState keyState, final RowDef rowDef,
			final RowData rowData);

	/**
	 * Invoked when a row update has been committed to a table.
	 * @param keyState
	 * @param rowDef
	 * @param oldRowData
	 * @param newRowData
	 */
	void updated(final KeyState keyState, final RowDef rowDef,
			final RowData oldRowData, final RowData newRowData);

	/**
	 * Invoked when deletion of a row has been committed.
	 * @param keyState
	 * @param rowDef
	 * @param rowData
	 */
	void deleted(final KeyState keyState, final RowDef rowDef,
			final RowData rowData);

}
