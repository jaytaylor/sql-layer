package com.akiban.cserver;

import com.akiban.cserver.message.CServerShutdownRequest;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ExecutionContext;

public interface CServerShutdownExecutionContext extends ExecutionContext {
	
	public void executeRequest(final AkibaConnection connection, final CServerShutdownRequest request) throws Exception;

}
