package com.akiban.cserver;

import com.akiban.cserver.message.ShutdownRequest;
import com.akiban.message.AkibaConnection;
import com.akiban.message.ExecutionContext;

public interface CServerShutdownExecutionContext extends ExecutionContext {
	
	public void executeRequest(final AkibaConnection connection, final ShutdownRequest request) throws Exception;

}
