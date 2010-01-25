package com.akiba.cserver.store;

import java.nio.ByteBuffer;

public interface RowDistributor {

	public boolean distributeNextRow(final ByteBuffer payload, final byte[] columnBitMap) throws Exception;
	
}
