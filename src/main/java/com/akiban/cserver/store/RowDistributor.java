package com.akiban.cserver.store;

import java.nio.ByteBuffer;

public interface RowDistributor {

	public boolean distributeNextRow(final ByteBuffer payload) throws Exception;
	
}
