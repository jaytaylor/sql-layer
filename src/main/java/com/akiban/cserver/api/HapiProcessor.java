package com.akiban.cserver.api;
import com.akiban.cserver.service.session.Session;

import java.nio.ByteBuffer;

public interface HapiProcessor {
	public String processRequest(Session session, String request ) ;
}
