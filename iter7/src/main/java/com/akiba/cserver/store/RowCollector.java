package com.akiba.cserver.store;

import java.nio.ByteBuffer;


public interface RowCollector {

	public boolean collectNextRow(final ByteBuffer payload) throws Exception;
	
	public boolean hasMore() throws Exception;
}
