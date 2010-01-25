package com.akiba.cserver.store;

import java.nio.ByteBuffer;


public interface RowCollector {

	public boolean collectNextRow(final ByteBuffer payload, final byte[] columnBitMap) throws Exception;
	
	public boolean hasMore() throws Exception;
}
